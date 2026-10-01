"""Performance lab 4: data skew. Usage: python -m src.perf.exp_skew"""
import statistics

from pyspark.sql import functions as F

from src.perf.measure import start_spark
from src.silver.common import silver_path


def profile(df, parts, cols):
    """Hash-partition df into `parts` partitions by `cols` (what the shuffle for a join or groupBy does) and count the
    rows in each. A stage finishes when its SLOWEST task finishes, so what matters is how big the biggest partition is
    compared with a typical one."""
    counts = {r["pid"]: r["n"] for r in df.repartition(parts, *cols)
              .groupBy(F.spark_partition_id().alias("pid")).agg(F.count("*").alias("n")).collect()}
    sizes = [counts.get(i, 0) for i in range(parts)]          # partitions that got no rows count as 0
    med = statistics.median(sizes)
    return max(sizes), med, (max(sizes) / med if med else float("inf")), sum(1 for s in sizes if s == 0)


def main():
    spark = start_spark("perf_skew")
    ships = (spark.read.format("delta").load(silver_path("shipments"))
             .select("shipment_id", "shipment_ref", "customer_id")
             # SALT: a number 0-9 derived from another column. Adding it to the key splits a hot key into up to 10 pieces.
             .withColumn("salt", F.pmod(F.hash("shipment_ref"), F.lit(10))))

    per_cust = ships.groupBy("customer_id").count()
    total, n_cust = ships.count(), per_cust.count()
    avg = total / n_cust
    print(f"\n{total:,} shipments over {n_cust} customers: average {avg:,.0f} per customer. The five biggest:")
    for r in per_cust.orderBy(F.desc("count")).limit(5).collect():
        print(f"  customer {r['customer_id']:>4}: {r['count']:>7,} shipments = {r['count'] / avg:5.1f}x the average")

    cases = [("by shipment_id (uniform keys)", ["shipment_id"]),
             ("by customer_id (skewed keys)", ["customer_id"]),
             ("by customer_id + salt (hot key spread over 10 buckets)", ["customer_id", "salt"])]
    for parts in (8, 200):
        print(f"\n--- {parts} partitions ---")
        print(f"{'partitioned':<58}{'largest':>9}{'median':>9}{'largest/median':>16}{'empty':>7}")
        for label, cols in cases:
            mx, med, ratio, empty = profile(ships, parts, cols)
            print(f"{label:<58}{mx:>9,.0f}{med:>9,.0f}{ratio:>15.1f}x{empty:>7}")

    print("\nSpark's own skew handling (adaptive execution, sort-merge joins only) kicks in only when a partition is both")
    print("several times the median AND larger than a byte threshold:")
    for k in ("spark.sql.adaptive.skewJoin.enabled", "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
              "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"):
        try:
            print(f"  {k} = {spark.conf.get(k)}")
        except Exception:
            print(f"  {k} = (not set in this Spark build)")
    spark.stop()


if __name__ == "__main__":
    main()
