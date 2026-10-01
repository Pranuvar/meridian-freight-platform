"""Gold: dim_customer (SCD Type 2). First run = initial load. Later runs = detect changes and MERGE."""
from delta.tables import DeltaTable

from src.bronze.common import new_batch_id
from src.gold.common import gold_path, log_gold
from src.gold.scd2 import initial_load, merge_changes
from src.silver.common import silver_path
from src.utils.spark import get_spark


def main():
    spark = get_spark("gold_dim_customer")
    batch = new_batch_id("run")
    src = (spark.read.format("delta").load(silver_path("customers"))
           .select("customer_id", "customer_code", "customer_name", "segment", "account_manager",
                   "region", "payment_terms_days", "status", "updated_at"))
    path = gold_path("dim_customer")

    if not DeltaTable.isDeltaTable(spark, path):
        initial_load(spark, path, src)
        print("dim_customer: initial load")
    else:
        stats = merge_changes(spark, path, src)
        print("dim_customer: merge ->", stats)

    n_out = spark.read.format("delta").load(path).count()
    log_gold(spark, batch, "dim_customer", src.count(), n_out)
    print(f"gold dim_customer rows={n_out:,} (all versions + 1 unknown member)")
    spark.stop()


if __name__ == "__main__":
    main()
