"""Silver checks: quarantine breakdown, standardisation results, late arrivals, broadcast plan."""
from pyspark.sql import functions as F

from src.silver.common import silver_path
from src.utils.config import CONTROL, QUARANTINE
from src.utils.spark import get_spark

spark = get_spark("silver_verify")


def silver(name):
    return spark.read.format("delta").load(silver_path(name))


print("\n--- quarantine: what was rejected, and why ---")
q = spark.read.format("delta").load(str(QUARANTINE / "rejected_records"))
q.groupBy("table_name", "reject_reasons").count().orderBy("table_name", "reject_reasons").show(50, False)

print("--- one quarantined record, with its full original content ---")
q.filter("table_name = 'shipments'").select("reject_reasons", "record").show(2, False)

print("--- scans: standardisation checks (all should be 0) ---")
s = silver("scans")
print("refs not in standard form :", s.filter(~F.col("shipment_ref").rlike(r"^MFL-\d{4}-\d{7}$")).count())
print("lowercase location codes  :", s.filter(F.col("location_code") != F.upper("location_code")).count())
print("null event_ts / event_type:", s.filter("event_ts IS NULL OR event_type IS NULL").count())

print("\n--- late-arriving scans: days between event and arrival ---")
s.groupBy("arrival_lag_days").count().orderBy("arrival_lag_days").show()

print("--- customers.segment after standardisation ---")
silver("customers").groupBy("segment").count().show()

print("--- scans that belong to a QUARANTINED shipment (still in Silver, on purpose) ---")
ships = silver("shipments").select("shipment_ref")
no_parent = s.join(F.broadcast(ships), "shipment_ref", "left_anti")
print("count:", no_parent.count(), "  <- Gold must handle these; the reconciliation will explain them")
print("\nPlan for that join. Look for BroadcastHashJoin (no Exchange on the big scans side):")
no_parent.explain()

print("--- Silver run log ---")
spark.read.format("delta").load(str(CONTROL / "run_log")).orderBy(F.desc("run_ts")) \
    .select("table_name", "records_in", "records_out", "records_rejected", "duplicates_removed").show(20, False)
spark.stop()
