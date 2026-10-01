"""Silver: FX rates and diesel index. Typed, validated, deduplicated."""
from pyspark.sql import functions as F

from src.bronze.common import new_batch_id
from src.quality.engine import reset_quarantine, split, write_quarantine
from src.quality.rules import Rule
from src.silver.common import account, dedupe, silver_path
from src.utils.config import bronze_path
from src.utils.spark import get_spark


def process(spark, batch, name, df, keys, rules):
    n_in = df.count()
    deduped = dedupe(df, keys, [F.col("_ingested_at").desc()]).persist()
    n_dups = n_in - deduped.count()
    clean, rejected = split(deduped, rules)
    clean = (clean.drop("_source_file", "_ingested_at", "_source_system", "_source_object", "_batch_id",
                        "_processing_date")
             .withColumn("_silver_batch_id", F.lit(batch)).withColumn("_silver_processed_at", F.current_timestamp()))
    out = silver_path(name)
    clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
    n_rej = rejected.count()
    reset_quarantine(spark, name)
    if n_rej:
        write_quarantine(rejected, name, "reference_api", batch)
    account(spark, batch, name, n_in, n_dups, n_rej, out)
    deduped.unpersist()


def main():
    spark = get_spark("silver_reference")
    batch = new_batch_id("initial")

    fx = (spark.read.format("delta").load(bronze_path("api_fx_rates"))
          .withColumn("rate_date", F.to_date("date")).withColumn("rate", F.col("rate").cast("decimal(10,4)"))
          .drop("date"))
    process(spark, batch, "fx_rates", fx, ["rate_date", "base", "quote"], [
        Rule("rate_date_invalid", F.col("rate_date").isNull(), "reject"),
        # A USD/INR rate outside 60-110 is a feed error, not a market move.
        Rule("rate_out_of_range", F.col("rate").isNull() | ~F.col("rate").between(60, 110), "reject"),
    ])

    fuel = (spark.read.format("delta").load(bronze_path("api_fuel_index"))
            .withColumn("week_start", F.to_date("week_start"))
            .withColumn("diesel_inr_per_litre", F.col("diesel_inr_per_litre").cast("decimal(8,2)")))
    process(spark, batch, "fuel_index", fuel, ["week_start"], [
        Rule("week_start_invalid", F.col("week_start").isNull(), "reject"),
        Rule("price_out_of_range", F.col("diesel_inr_per_litre").isNull()
             | ~F.col("diesel_inr_per_litre").between(60, 150), "reject"),
    ])
    spark.stop()


if __name__ == "__main__":
    main()
