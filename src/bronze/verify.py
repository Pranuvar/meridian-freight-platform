"""Bronze sanity check: does Bronze contain exactly what the sources contained?"""
from src.utils.config import PG_PROPS, PG_URL, bronze_path
from src.utils.spark import get_spark

spark = get_spark("bronze_verify")


def bronze_count(name):
    return spark.read.format("delta").load(bronze_path(name)).count()


def pg_count(table):
    q = f"(SELECT COUNT(*) AS c FROM tms.{table}) t"
    return spark.read.jdbc(PG_URL, q, properties=PG_PROPS).first()["c"]


results = []
for t in ["locations", "customers", "carriers", "shipments", "shipment_charges", "claims"]:
    results.append((f"tms_{t}", pg_count(t), bronze_count(f"tms_{t}")))

# File sources: the expected numbers are what the generator printed.
results.append(("scans", 1_984_067, bronze_count("scans")))
results.append(("invoices (3 tables)", 405_674, sum(bronze_count(f"invoices_{c}") for c in ["blz", "swf", "trk"])))

print(f"\n{'table':<24}{'source':>12}{'bronze':>12}  result")
for name, src, brz in results:
    print(f"{name:<24}{src:>12,}{brz:>12,}  {'PASS' if src == brz else 'FAIL'}")

print("\n--- scans schema and sample (note _arrival_date and _source_file) ---")
s = spark.read.format("delta").load(bronze_path("scans"))
s.printSchema()
s.select("shipment_ref", "event_type", "event_ts", "location_code", "scan_source", "_arrival_date").show(8, False)
print("rows with null scan_source (from the early schema-drift files):",
      s.filter("scan_source IS NULL").count())
spark.stop()
