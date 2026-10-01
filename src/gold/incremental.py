"""Gold, INCREMENTAL facts: find what changed, recompute only that, MERGE.

  python -m src.gold.incremental                                   normal run (uses the Gold watermarks)
  python -m src.gold.incremental --replay                          re-process everything since the last FULL build
                                                                   (idempotency proof; watermarks untouched)
  python -m src.gold.incremental --backfill 2026-06-01 2026-06-07  recompute shipments BOOKED in that window
                                                                   (a correction, not new data: watermarks untouched)

Run order matters: dim_customer FIRST (python -m src.gold.build_dim_customer), then this.
"""
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.bronze.common import new_batch_id
from src.gold.build_facts import build_fact_invoice, build_fact_shipment, build_fact_tracking, read_silver
from src.gold.common import gold_path, log_gold
from src.utils.spark import get_spark
from src.utils.state import FMT, get_ts, max_ts, set_ts


# ---------------------------------------------------------------- what changed?
def since(df, wm):
    """Silver rows written or updated AFTER the watermark. _silver_processed_at is stamped by the Silver jobs
    only on rows they inserted or updated, so it doubles as the change marker Gold needs."""
    return df.filter(F.col("_silver_processed_at") > F.lit(wm).cast("timestamp"))


def find_affected(ships, charges, claims, scans, invoices, wm):
    """Shipments whose fact row must be recomputed. Returns (shipment_id, shipment_ref).
    Charges and claims are keyed by shipment_id, scans and invoices by shipment_ref, so each side is translated
    through Silver shipments. The inner join also drops refs Silver has no shipment for (orphans)."""
    dictionary = ships.select("shipment_id", "shipment_ref")
    by_ref = (since(ships, wm).select("shipment_ref")
              .unionByName(since(scans, wm).select("shipment_ref"))
              .unionByName(since(invoices, wm).select("shipment_ref"))
              .distinct().join(dictionary, "shipment_ref", "inner"))
    by_id = (since(charges, wm).select("shipment_id")
             .unionByName(since(claims, wm).select("shipment_id"))
             .distinct().join(dictionary, "shipment_id", "inner"))
    return (by_ref.select("shipment_id", "shipment_ref")
            .unionByName(by_id.select("shipment_id", "shipment_ref")).distinct())


def consumed_watermark(old, *frames):
    """Highest _silver_processed_at among the rows this run consumed. Never moves backwards."""
    vals = [v for v in (max_ts(since(f, old), "_silver_processed_at") for f in frames) if v]
    return max(vals) if vals else old


def start_wm(spark, key, path, replay):
    """Saved watermark, else the time of the EARLIEST full-build row. MIN, not MAX: if a first run crashed after its
    merge but before saving its watermark, MAX would point past data it never finished and skip it forever."""
    wm = None if replay else get_ts(spark, key)
    if wm is None:
        t = spark.read.format("delta").load(path)
        wm = t.agg(F.date_format(F.min("_gold_processed_at"), FMT)).first()[0]
        if not replay:
            print(f"  [{key}] no watermark yet: starting from the first full build, {wm}")
    return wm


# ---------------------------------------------------------------- merging
def run_merge(spark, name, build):
    """Execute a Delta MERGE and return (inserted, updated) from Delta's own metrics. A merge that changes nothing may
    not create a commit, so we compare versions rather than trust whatever commit is latest."""
    dt = DeltaTable.forPath(spark, gold_path(name))
    v0 = dt.history(1).select("version").first()[0]
    build(dt).execute()
    h = dt.history(1).select("version", "operationMetrics").first()
    if h["version"] == v0:
        return 0, 0
    m = h["operationMetrics"] or {}
    return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))


def upsert(spark, name, key, df, update_condition=None):
    """Matched -> update (optionally only when something really changed). Not matched -> insert."""
    return run_merge(spark, name, lambda dt: dt.alias("t").merge(df.alias("s"), f"t.{key} = s.{key}")
                     .whenMatchedUpdateAll(condition=update_condition).whenNotMatchedInsertAll())


def insert_only(spark, name, key, df):
    """No 'when matched' clause: keys already present are ignored."""
    return run_merge(spark, name, lambda dt: dt.alias("t").merge(df.alias("s"), f"t.{key} = s.{key}")
                     .whenNotMatchedInsertAll())


def check_grain(spark, name, key, expected):
    t = spark.read.format("delta").load(gold_path(name))
    n, k = t.count(), t.select(key).distinct().count()
    ok = n == k == expected
    print(f"  {name:<28} rows={n:>10,}  distinct keys={k:>10,}  silver rows={expected:>10,}  {'PASS' if ok else 'FAIL'}")
    return ok


def main():
    args = sys.argv[1:]
    replay = "--replay" in args
    backfill = tuple(args[args.index("--backfill") + 1: args.index("--backfill") + 3]) if "--backfill" in args else None
    advance = not replay and backfill is None            # only a normal run may move the watermarks
    spark = get_spark("gold_incremental")
    batch = new_batch_id("ginc")
    s = {n: read_silver(spark, n) for n in
         ["shipments", "shipment_charges", "claims", "scans", "carrier_invoice_lines"]}
    print(f"gold incremental run {batch}" + (" [REPLAY]" if replay else "") + (f" [BACKFILL {backfill}]" if backfill else ""))
    ok = True

    # ============================================================ fact_shipment
    path, key = gold_path("fact_shipment"), "gold.fact_shipment"
    new_wm = None
    if backfill:
        affected = (s["shipments"].filter(F.to_date("booked_at").between(*backfill))
                    .select("shipment_id", "shipment_ref"))
    else:
        wm = start_wm(spark, key, path, replay)
        affected = find_affected(s["shipments"], s["shipment_charges"], s["claims"], s["scans"],
                                 s["carrier_invoice_lines"], wm)
        new_wm = consumed_watermark(wm, *s.values())
    affected = affected.persist()        # used by the count AND the build, so compute it once
    n_aff, total = affected.count(), s["shipments"].count()
    if n_aff == 0:
        print("  fact_shipment                nothing to do")
    else:
        fresh = build_fact_shipment(spark, batch, refs=affected)
        ins, upd = upsert(spark, "fact_shipment", "shipment_key", fresh)
        print(f"  fact_shipment                affected shipments={n_aff:>8,} ({n_aff / total:.1%} of {total:,})"
              f"  -> inserted {ins:,}, updated {upd:,}")
        log_gold(spark, batch, "fact_shipment", n_aff, ins + upd)
        ok &= (ins + upd == n_aff)       # the rebuilt set must land 1:1, or the grain is broken
    ok &= check_grain(spark, "fact_shipment", "shipment_key", total)
    if advance and ok and new_wm:
        set_ts(spark, key, new_wm)
    affected.unpersist()
    if backfill:                          # a backfill only rebuilds shipments
        spark.stop()
        sys.exit(0 if ok else "FAILED")

    # ============================================================ fact_tracking_event
    path, key = gold_path("fact_tracking_event"), "gold.fact_tracking_event"
    wm = start_wm(spark, key, path, replay)
    new_scans = since(s["scans"], wm)
    new_wm = max_ts(new_scans, "_silver_processed_at")
    refs = new_scans.select("shipment_ref").distinct().persist()
    n_refs = refs.count()
    if n_refs == 0:
        print("  fact_tracking_event          nothing to do")
    else:
        fresh = build_fact_tracking(spark, batch, refs=refs)      # ALL scans of these shipments (window needs them)
        # Silver scans never change, so the only value that can differ on an existing event is the gap to its
        # predecessor. Updating only when that really changed counts the neighbours a late scan disturbed.
        ins, upd = upsert(spark, "fact_tracking_event", "event_key", fresh,
                          update_condition="NOT (t.hours_since_prev_scan <=> s.hours_since_prev_scan)")
        print(f"  fact_tracking_event          shipments with new scans={n_refs:>8,}"
              f"  -> inserted {ins:,} events, re-gapped {upd:,} existing events")
        log_gold(spark, batch, "fact_tracking_event", n_refs, ins + upd)
    ok &= check_grain(spark, "fact_tracking_event", "event_key", s["scans"].count())
    if advance and ok and new_wm:
        set_ts(spark, key, new_wm)
    refs.unpersist()

    # ============================================================ fact_carrier_invoice_line
    path, key = gold_path("fact_carrier_invoice_line"), "gold.fact_carrier_invoice_line"
    wm = start_wm(spark, key, path, replay)
    new_lines = since(s["carrier_invoice_lines"], wm)
    new_wm, n_new = max_ts(new_lines, "_silver_processed_at"), new_lines.count()
    if n_new == 0:
        print("  fact_carrier_invoice_line    nothing to do")
    else:
        ins, _ = insert_only(spark, "fact_carrier_invoice_line", "invoice_line_key",
                             build_fact_invoice(spark, batch, since=wm))
        expect = 0 if replay else n_new           # a replay sees lines that are already there
        print(f"  fact_carrier_invoice_line    new Silver lines={n_new:>8,}  -> inserted {ins:,}"
              + ("  (replay: must be 0)" if replay else ""))
        log_gold(spark, batch, "fact_carrier_invoice_line", n_new, ins)
        ok &= (ins == expect)
    ok &= check_grain(spark, "fact_carrier_invoice_line", "invoice_line_key", s["carrier_invoice_lines"].count())
    if advance and ok and new_wm:
        set_ts(spark, key, new_wm)

    spark.stop()
    if not ok:
        sys.exit("one or more Gold checks FAILED. Watermarks were not advanced for any fact that failed.")


if __name__ == "__main__":
    main()
