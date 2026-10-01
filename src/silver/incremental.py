"""Silver, INCREMENTAL. Reads only Bronze rows newer than the layer watermark, validates them with the same
rules as the full build, then MERGEs into Silver. Two merge styles:
  upsert_merge       UPDATE rows whose incoming version is STRICTLY newer, INSERT new keys   (shipments, charges, claims)
  insert_only_merge  INSERT keys that are not there yet, ignore everything already present   (scans, invoice lines)
Every job ends by proving the books balance, and only THEN advances its watermark.
"""
import re
from datetime import datetime, timezone
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.bronze.common import new_batch_id
from src.quality.engine import split, write_quarantine
from src.quality.rules import customer_rules, normalise_ref, parse_event_ts, scan_rules, shipment_rules
from src.quality.rules_silver import charge_rules, claim_rules, invoice_rules, orphan_rule
from src.silver.build_invoices import parse_blz, parse_swf, parse_trk
from src.silver.build_tms import process, std_carriers, std_charges, std_claims, std_customers, std_shipments
from src.silver.common import RUN_LOG_SCHEMA, dedupe, silver_path
from src.utils.config import CONTROL, QUARANTINE, bronze_path
from src.utils.spark import get_spark
from src.utils.state import get_ts, max_ts, set_ts


# ---------------------------------------------------------------- the two merge styles
def upsert_merge(spark, path, df, key):
    """Matched AND incoming is strictly newer -> update. Matched but equal/older -> leave alone (this is
    what makes replays harmless). Not matched -> insert."""
    (DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll().execute())


def insert_only_merge(spark, path, df, key_cols):
    """No 'when matched' clause: rows whose key already exists are ignored. <=> is null-safe equality,
    so a NULL key column matches a NULL instead of silently never matching."""
    cond = " AND ".join(f"t.{c} <=> s.{c}" for c in key_cols)
    (DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), cond)
        .whenNotMatchedInsertAll().execute())


# ---------------------------------------------------------------- shared plumbing
def bronze_increment(spark, bronze_name, layer_key):
    """Bronze rows ingested AFTER the layer watermark. Arrival time, not event time: a late record has an
    old business timestamp but a new _ingested_at."""
    wm = get_ts(spark, f"silver.{layer_key}")
    if wm is None:
        raise RuntimeError(f"no Silver watermark for {layer_key}: run python -m src.utils.bootstrap_state first")
    b = spark.read.format("delta").load(bronze_path(bronze_name))
    return b.filter(F.col("_ingested_at") > F.lit(wm).cast("timestamp")), wm


def batch_id(table, new_wm):
    """Deterministic: the same increment always gets the same id, so a retry replaces its own quarantine rows."""
    return f"inc_{table}_{re.sub(r'[^0-9]', '', new_wm)}"


def merge_metrics(spark, path):
    return DeltaTable.forPath(spark, path).history(1).select("operationMetrics").first()[0] or {}


def apply_and_account(spark, table, batch, source, n_in, n_dup_batch, clean, rejected, merge):
    """Shared tail of every job: merge the clean rows, quarantine the rejects, prove nothing was lost."""
    path = silver_path(table)
    n_clean, n_rej = clean.count(), rejected.count()
    before = spark.read.format("delta").load(path).count()
    m = {}
    if n_clean:
        merge(path, clean)
        m = merge_metrics(spark, path)
    after = spark.read.format("delta").load(path).count()

    qpath = str(QUARANTINE / "rejected_records")
    if DeltaTable.isDeltaTable(spark, qpath):            # a retry of the same increment replaces its own rows
        DeltaTable.forPath(spark, qpath).delete(f"table_name = '{table}' AND batch_id = '{batch}'")
    if n_rej:
        write_quarantine(rejected, table, source, batch)

    inserted, updated = after - before, int(m.get("numTargetRowsUpdated", 0))
    ignored = n_clean - inserted - updated               # already present, or an older version: safely skipped
    n_dups = n_dup_batch + ignored
    ok = (n_in == n_dup_batch + n_clean + n_rej          # nothing lost between Bronze and the merge
          and ignored >= 0
          and int(m.get("numTargetRowsInserted", inserted)) == inserted)   # Delta's own count agrees with table growth
    print(f"silver {table:<22} in={n_in:>8,} = inserted={inserted:>7,} + updated={updated:>7,} + "
          f"rejected={n_rej:>6,} + duplicates/stale={n_dups:>7,}  {'PASS' if ok else 'FAIL'}")
    row = [(datetime.now(timezone.utc), batch, "silver", table, int(n_in), int(inserted + updated),
            int(n_rej), int(n_dups))]
    spark.createDataFrame(row, RUN_LOG_SCHEMA).write.format("delta").mode("append").save(str(CONTROL / "run_log"))
    if not ok:
        raise RuntimeError(f"{table}: accounting failed, watermark NOT advanced")


# ---------------------------------------------------------------- small tables: cheap to rebuild from append-only Bronze
def rebuild_small(spark):
    batch = new_batch_id("inc")
    latest = [F.col("updated_at").desc(), F.col("_ingested_at").desc()]
    b = lambda n: spark.read.format("delta").load(bronze_path(n))      # noqa: E731
    process(spark, batch, "carriers", b("tms_carriers"), ["carrier_id"], latest, [], std_carriers)
    process(spark, batch, "customers", b("tms_customers"), ["customer_id"], latest, customer_rules(), std_customers)


# ---------------------------------------------------------------- big TMS tables: upsert
def upsert_table(spark, table, bronze_name, key, rules, std, parent_ids=None):
    inc, wm = bronze_increment(spark, bronze_name, table)
    new_wm = max_ts(inc, "_ingested_at")
    if new_wm is None:
        print(f"silver {table:<22} nothing new since {wm}")
        return
    batch = batch_id(table, new_wm)
    inc = inc.persist()
    n_in = inc.count()
    # Several versions of one key (or a re-read after a crash) collapse to the NEWEST one.
    deduped = dedupe(inc, [key], [F.col("updated_at").desc(), F.col("_ingested_at").desc()]).persist()
    n_dup_batch = n_in - deduped.count()
    checked = deduped.join(F.broadcast(parent_ids), "shipment_id", "left") if parent_ids is not None else deduped
    clean, rejected = split(checked, rules)
    if parent_ids is not None:
        clean, rejected = clean.drop("_known_parent"), rejected.drop("_known_parent")
    clean = (std(clean).withColumn("_silver_batch_id", F.lit(batch))
                       .withColumn("_silver_processed_at", F.current_timestamp())).persist()
    apply_and_account(spark, table, batch, "tms_postgres", n_in, n_dup_batch, clean, rejected,
                      lambda p, df: upsert_merge(spark, p, df, key))
    set_ts(spark, f"silver.{table}", new_wm)             # LAST step: only after the books balanced
    for d in (inc, deduped, clean):
        d.unpersist()


# ---------------------------------------------------------------- scans: insert-only
def shape_scans(clean, batch):
    return clean.select(
        F.col("_ref_std").alias("shipment_ref"), "event_type",
        F.col("_ts").alias("event_ts"), F.to_date("_ts").alias("event_date"),
        F.col("_loc_std").alias("location_code"), "scan_source",
        F.col("_arrival_date").alias("arrival_date"),
        F.datediff(F.col("_arrival_date"), F.to_date("_ts")).alias("arrival_lag_days"),
        "_source_file", F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.lit(batch).alias("_silver_batch_id"), F.current_timestamp().alias("_silver_processed_at"),
        "_dq_warn_reasons")


def scans_incremental(spark):
    inc, wm = bronze_increment(spark, "scans", "scans")
    new_wm = max_ts(inc, "_ingested_at")
    if new_wm is None:
        print(f"silver {'scans':<22} nothing new since {wm}")
        return
    batch = batch_id("scans", new_wm)
    inc = inc.persist()
    n_in = inc.count()
    valid_locs = [r["location_code"] for r in
                  spark.read.format("delta").load(silver_path("locations")).select("location_code").collect()]
    # Known shipments come from BRONZE (which now includes the new week), so a shipment rejected for a bad weight
    # still counts as existing and its scans are not orphans.
    known = (spark.read.format("delta").load(bronze_path("tms_shipments"))
             .select(F.col("shipment_ref").alias("_ref_std")).distinct().withColumn("_known_ref", F.lit(1)))
    prepared = (inc.withColumn("_ref_std", normalise_ref(F.col("shipment_ref")))
                   .withColumn("_ts", parse_event_ts())
                   .withColumn("_loc_std", F.upper(F.trim("location_code"))))
    deduped = dedupe(prepared, ["_ref_std", "event_type", "_ts", "_loc_std"],
                     [F.col("_arrival_date").asc(), F.col("_ingested_at").asc()]).persist()
    n_dup_batch = n_in - deduped.count()
    clean, rejected = split(deduped.join(F.broadcast(known), "_ref_std", "left"),
                            scan_rules(valid_locs) + [orphan_rule()])
    clean = shape_scans(clean, batch).persist()
    rejected = rejected.drop("_known_ref", "_ref_std", "_ts", "_loc_std")
    apply_and_account(spark, "scans", batch, "scan_files", n_in, n_dup_batch, clean, rejected,
                      lambda p, df: insert_only_merge(spark, p, df,
                                                      ["shipment_ref", "event_type", "event_ts", "location_code"]))
    set_ts(spark, "silver.scans", new_wm)
    for d in (inc, deduped, clean):
        d.unpersist()


# ---------------------------------------------------------------- invoices: insert-only
def invoices_incremental(spark):
    parts, new_wms = [], {}
    for code, parser in [("blz", parse_blz), ("swf", parse_swf), ("trk", parse_trk)]:
        inc, _ = bronze_increment(spark, f"invoices_{code}", f"invoices_{code}")
        nw = max_ts(inc, "_ingested_at")
        if nw is not None:
            parts.append(parser(inc))
            new_wms[f"invoices_{code}"] = nw
    if not parts:
        print(f"silver {'carrier_invoice_lines':<22} nothing new since watermark")
        return
    batch = batch_id("carrier_invoice_lines", max(new_wms.values()))
    unified = reduce(lambda a, b: a.unionByName(b), parts).persist()
    n_in = unified.count()
    known = (spark.read.format("delta").load(bronze_path("tms_shipments"))
             .select(F.col("shipment_ref").alias("_ref_std")).distinct().withColumn("_known_ref", F.lit(1)))
    prepared = unified.withColumn("_ref_std", normalise_ref(F.col("_ref_raw")))
    deduped = dedupe(prepared, ["carrier_code", "invoice_no", "_ref_std", "charge_type"],
                     [F.col("_ingested_at").asc(), F.col("_source_file").asc()]).persist()
    n_dup_batch = n_in - deduped.count()
    clean, rejected = split(deduped.join(F.broadcast(known), "_ref_std", "left"), invoice_rules())
    clean = clean.select(
        "carrier_code", "invoice_no", "invoice_date", F.col("_ref_std").alias("shipment_ref"),
        "charge_type", "amount", "gst_amount", "currency", "_source_file",
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.lit(batch).alias("_silver_batch_id"), F.current_timestamp().alias("_silver_processed_at"),
        "_dq_warn_reasons").persist()
    rejected = rejected.drop("_known_ref", "_ref_std")
    apply_and_account(spark, "carrier_invoice_lines", batch, "carrier_invoices", n_in, n_dup_batch, clean, rejected,
                      lambda p, df: insert_only_merge(spark, p, df,
                                                      ["carrier_code", "invoice_no", "shipment_ref", "charge_type"]))
    for k, v in new_wms.items():
        set_ts(spark, f"silver.{k}", v)
    for d in (unified, deduped, clean):
        d.unpersist()


def main():
    spark = get_spark("silver_incremental")
    rebuild_small(spark)
    upsert_table(spark, "shipments", "tms_shipments", "shipment_id", shipment_rules(), std_shipments)
    # Children validate against the shipments that survived into Silver, AFTER this run's shipment merge.
    known = (spark.read.format("delta").load(silver_path("shipments"))
             .select("shipment_id").distinct().withColumn("_known_parent", F.lit(1)))
    upsert_table(spark, "shipment_charges", "tms_shipment_charges", "charge_id", charge_rules(), std_charges,
                 parent_ids=known)
    upsert_table(spark, "claims", "tms_claims", "claim_id", claim_rules(), std_claims, parent_ids=known)
    scans_incremental(spark)
    invoices_incremental(spark)
    spark.stop()


if __name__ == "__main__":
    main()
