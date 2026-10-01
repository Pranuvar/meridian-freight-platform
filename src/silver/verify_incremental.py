"""Checks after an incremental run: state, Bronze batches, Silver accounting, late data, uniqueness."""
from pyspark.sql import functions as F

from src.silver.common import silver_path
from src.utils.config import CONTROL, bronze_path
from src.utils.spark import get_spark
from src.utils.state import FMT, STATE

spark = get_spark("silver_incremental_verify")
silver = lambda n: spark.read.format("delta").load(silver_path(n))      # noqa: E731
bronze = lambda n: spark.read.format("delta").load(bronze_path(n))      # noqa: E731

print("\n--- watermarks (control table) ---")
spark.read.format("delta").load(STATE).select("key", F.date_format("ts", FMT).alias("watermark")) \
    .orderBy("key").show(truncate=False)

print("--- Bronze is append-only: rows per batch (a crash/recovery run shows up as repeated rows) ---")
for name in ["tms_customers", "tms_shipments", "tms_shipment_charges", "scans"]:
    print(name)
    bronze(name).groupBy("_batch_id").count().orderBy("_batch_id").show(truncate=False)

print("--- Silver run log, latest entries ---")
(spark.read.format("delta").load(str(CONTROL / "run_log")).filter("layer = 'silver'")
    .orderBy(F.desc("run_ts"))
    .select("run_ts", "table_name", "records_in", "records_out", "records_rejected", "duplicates_removed")
    .show(14, False))

print("--- Silver shipments ---")
s = silver("shipments")
s.groupBy("status").count().orderBy("status").show()
print("booked on/after 2026-06-01:", s.filter("booked_at >= '2026-06-01'").count())

print("--- late-arriving scans now in Silver (event on/before 31 May, arrived in June) ---")
print(silver("scans").filter("arrival_date >= '2026-06-01' AND event_date <= '2026-05-31'").count())

print("--- uniqueness: rows must equal distinct keys ---")
for name, keys in [("shipments", ["shipment_id"]), ("shipment_charges", ["charge_id"]),
                   ("scans", ["shipment_ref", "event_type", "event_ts", "location_code"]),
                   ("carrier_invoice_lines", ["carrier_code", "invoice_no", "shipment_ref", "charge_type"])]:
    t = silver(name)
    n, k = t.count(), t.select(*keys).distinct().count()
    print(f"{name:<24} rows={n:>9,}  distinct keys={k:>9,}  {'PASS' if n == k else 'FAIL'}")

print("\n--- customers reassigned on 2026-06-03 (Silver keeps the latest; Gold SCD2 keeps both) ---")
print(silver("customers").filter("updated_at >= '2026-06-03'").count())
spark.stop()
