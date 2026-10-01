"""Gold dimension checks: key uniqueness, unknown members, and SCD2 interval integrity."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.gold.common import FAR_FUTURE, FAR_PAST, gold_path
from src.utils.spark import get_spark

spark = get_spark("gold_dims_verify")


def gold(name):
    return spark.read.format("delta").load(gold_path(name))


print("\n--- sizes, key uniqueness, unknown member ---")
for name, key in [("dim_date", "date_key"), ("dim_location", "location_key"), ("dim_carrier", "carrier_key"),
                  ("dim_service_level", "service_level_key"), ("dim_lane", "lane_key"),
                  ("dim_customer", "customer_key")]:
    d = gold(name)
    n, k = d.count(), d.select(key).distinct().count()
    unk = d.filter(F.col(key) == -1).count()
    print(f"{name:<20} rows={n:>6,}  distinct keys={k:>6,} {'PASS' if n == k else 'FAIL'}   unknown member rows={unk}")

print("\n--- dim_customer: SCD2 integrity (every count below should be 0, or PASS) ---")
c = gold("dim_customer").filter("customer_id <> -1")
w = Window.partitionBy("customer_id").orderBy("effective_from")
chk = c.withColumn("next_from", F.lead("effective_from").over(w)).withColumn("rn", F.row_number().over(w))
n_cust = c.select("customer_id").distinct().count()
not_one_current = (c.groupBy("customer_id").agg(F.sum(F.col("is_current").cast("int")).alias("cur"))
                   .filter("cur <> 1").count())
print(f"customers={n_cust:,}; customers without exactly 1 current row: {not_one_current}")
print("gaps/overlaps between consecutive versions:",
      chk.filter(F.col("next_from").isNotNull() & (F.col("effective_to") != F.col("next_from"))).count())
print("current version not ending 9999-12-31     :",
      chk.filter(F.col("next_from").isNull() & (F.col("effective_to") != F.to_date(F.lit(FAR_FUTURE)))).count())
print("first version not starting 1900-01-01     :",
      chk.filter((F.col("rn") == 1) & (F.col("effective_from") != F.to_date(F.lit(FAR_PAST)))).count())
print("zero/negative length versions             :", c.filter("effective_to <= effective_from").count())

print("\n--- versions per customer ---")
per = c.groupBy("customer_id").agg(F.count("*").alias("versions"))
per.groupBy("versions").agg(F.count("*").alias("customers")).orderBy("versions").show()

top = per.orderBy(F.desc("versions"), "customer_id").first()["customer_id"]
print(f"--- full history of customer_id {top} ---")
c.filter(F.col("customer_id") == top).orderBy("effective_from").select(
    "customer_code", "account_manager", "region", "effective_from", "effective_to", "is_current",
    "customer_key").show(truncate=False)

print("--- dim_date sample ---")
gold("dim_date").filter("date IN ('2026-03-31', '2026-04-01')").orderBy("date").show(truncate=False)
spark.stop()
