"""Serving export: the PUBLISHED Gold tables as single Parquet files for Power BI (Import mode).
Reads data/control/published.json and uses Delta time travel (versionAsOf), so Power BI sees exactly the version that
passed reconciliation, even if a later run has already started changing Gold.
  python -m src.serve.export        (set SERVE_DIR to write somewhere else)"""
import json
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import functions as F

from src.gold.common import gold_path
from src.utils.config import CONTROL, DATA, QUARANTINE
from src.utils.spark import get_spark

SERVE = Path(os.getenv("SERVE_DIR", str(DATA / "serve")))
# Governed assumptions, kept in ONE table so no measure hard-codes them (see the metric dictionary).
SCAN_LAG_DAYS, INVOICE_LAG_DAYS, CLAIM_LAG_DAYS = 5, 30, 10

TABLES = ["dim_date", "dim_location", "dim_carrier", "dim_service_level", "dim_lane", "dim_customer",
          "fact_shipment", "fact_tracking_event", "fact_carrier_invoice_line"]
# Columns the model does not need. Fewer, lower-cardinality columns = a smaller and faster model.
DROP = {
    "fact_shipment": ["shipment_id", "source_updated_at", "_gold_batch_id", "_gold_processed_at"],
    "fact_tracking_event": ["event_key", "shipment_ref", "_gold_batch_id", "_gold_processed_at"],
    "fact_carrier_invoice_line": ["invoice_line_key", "shipment_ref", "_gold_batch_id", "_gold_processed_at"],
    "dim_customer": ["row_hash"],
}


def write_single(df, name):
    """One Parquet FILE per table: Power BI's Parquet connector reads a file, not a Spark output folder."""
    tmp, out = SERVE / f"_tmp_{name}", SERVE / f"{name}.parquet"
    shutil.rmtree(tmp, ignore_errors=True)
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp))
    part = next(tmp.glob("part-*.parquet"))             # Spark names its output part-0000-<uuid>.parquet
    out.unlink(missing_ok=True)
    shutil.move(str(part), str(out))
    shutil.rmtree(tmp)
    return out


def main():
    marker_file = CONTROL / "published.json"
    if not marker_file.exists():
        sys.exit("nothing has been published yet: run the pipeline first")
    marker = json.loads(marker_file.read_text())
    versions = marker["gold_table_versions"]
    spark = get_spark("serve_export")
    # Spark's default timestamp encoding (INT96) is a legacy type some readers cannot parse. Use the standard one.
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    SERVE.mkdir(parents=True, exist_ok=True)

    frames = {}
    for name in TABLES:        # versionAsOf = the exact Delta version that was published
        df = spark.read.format("delta").option("versionAsOf", versions[name]).load(gold_path(name))
        frames[name] = df.drop(*DROP.get(name, []))

    # Customer: keep the SCD2 as-of attributes AND add who owns the account today.
    cust = frames["dim_customer"]
    current = (cust.filter("is_current").select("customer_id", F.col("account_manager").alias("account_manager_current"),
                                                 F.col("region").alias("region_current")))
    enriched = cust.join(F.broadcast(current), "customer_id", "left")     # exactly one current row per customer
    assert enriched.count() == cust.count(), "customer enrichment changed the row count"
    frames["dim_customer"] = enriched

    # Data Status: one row describing what Power BI is looking at.
    fs, inv, ev = frames["fact_shipment"], frames["fact_carrier_invoice_line"], frames["fact_tracking_event"]
    quarantined = (spark.read.format("delta").load(str(QUARANTINE / "rejected_records")).filter("table_name = 'shipments'")
                   .select(F.get_json_object("record", "$.shipment_id")).distinct().count())
    unmatched = inv.join(fs.select("shipment_key"), "shipment_key", "left_anti").agg(F.sum("amount_inr")).first()[0] or 0
    frames["data_status"] = spark.createDataFrame(
        [(marker["published_at"], marker["run_id"], int(ev.agg(F.max("event_date_key")).first()[0]),
          int(fs.agg(F.max("booked_date_key")).first()[0]), SCAN_LAG_DAYS, INVOICE_LAG_DAYS, CLAIM_LAG_DAYS,
          quarantined, float(unmatched))],
        "published_at string, run_id string, latest_event_date_key int, latest_booking_date_key int, scan_lag_days int, "
        "invoice_lag_days int, claim_lag_days int, shipments_quarantined long, invoice_cost_unmatched_inr double")

    print(f"exporting published run {marker['run_id']} (published {marker['published_at']}) to {SERVE}\n")
    print(f"{'table':<28}{'rows':>12}{'in file':>12}  check")
    for name, df in frames.items():
        n = df.count()
        out = write_single(df, name)
        n_out = spark.read.parquet(str(out)).count()          # read the FILE back: prove what Power BI will see
        print(f"{name:<28}{n:>12,}{n_out:>12,}  {'PASS' if n == n_out else 'FAIL'}   ({out.stat().st_size / 1e6:.1f} MB)")
    total = spark.read.parquet(str(SERVE / "fact_shipment.parquet")).agg(F.sum("revenue_total_inr")).first()[0]
    print(f"\nrevenue control total in the exported file: Rs {total:,.2f}")
    print(f"quarantined shipments excluded: {quarantined:,} | carrier invoice cost with no shipment in the model: Rs {float(unmatched):,.2f}")
    spark.stop()


if __name__ == "__main__":
    main()
