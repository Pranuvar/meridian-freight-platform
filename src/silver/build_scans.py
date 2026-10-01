"""Silver: tracking scans. Order matters: standardise keys -> dedupe -> validate (incl. orphans)
-> write clean / quarantine rejected."""
from pyspark.sql import functions as F

from src.bronze.common import new_batch_id
from src.quality.engine import reset_quarantine, split, write_quarantine
from src.quality.rules import normalise_ref, parse_event_ts, scan_rules
from src.quality.rules_silver import orphan_rule
from src.silver.common import account, dedupe, silver_path
from src.utils.config import bronze_path
from src.utils.spark import get_spark


def main():
    spark = get_spark("silver_scans")
    batch = new_batch_id("initial")

    bronze = spark.read.format("delta").load(bronze_path("scans"))
    n_in = bronze.count()
    # Only 18 rows, so collect() is safe here.
    valid_locs = [r["location_code"] for r in
                  spark.read.format("delta").load(silver_path("locations")).select("location_code").collect()]
    # Every shipment the TMS has ever known (from BRONZE, so shipments rejected for a bad weight
    # still count as existing; their scans are not orphans).
    known = (spark.read.format("delta").load(bronze_path("tms_shipments"))
             .select(F.col("shipment_ref").alias("_ref_std")).distinct()
             .withColumn("_known_ref", F.lit(1)))

    # 1) Standardised copies of the key columns. The RAW columns stay untouched for the rules.
    prepared = (bronze
                .withColumn("_ref_std", normalise_ref(F.col("shipment_ref")))
                .withColumn("_ts", parse_event_ts())
                .withColumn("_loc_std", F.upper(F.trim("location_code"))))

    # 2) Dedupe on the STANDARDISED key, so "MFL20250000123" and "MFL-2025-0000123" are recognised
    #    as the same scan. Earliest arrival wins.
    deduped = dedupe(prepared, ["_ref_std", "event_type", "_ts", "_loc_std"],
                     [F.col("_arrival_date").asc(), F.col("_ingested_at").asc()]).persist()
    n_dups = n_in - deduped.count()

    # 3) Validate. Broadcast join adds _known_ref (NULL = orphan), which the orphan rule reads.
    joined = deduped.join(F.broadcast(known), "_ref_std", "left")
    clean, rejected = split(joined, scan_rules(valid_locs) + [orphan_rule()])

    # 4) Shape the Silver table.
    silver = clean.select(
        F.col("_ref_std").alias("shipment_ref"),
        "event_type",
        F.col("_ts").alias("event_ts"),
        F.to_date("_ts").alias("event_date"),
        F.col("_loc_std").alias("location_code"),
        "scan_source",
        F.col("_arrival_date").alias("arrival_date"),
        # Late-arriving data made measurable: days between the event and the file it arrived in.
        F.datediff(F.col("_arrival_date"), F.to_date("_ts")).alias("arrival_lag_days"),
        "_source_file",
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.lit(batch).alias("_silver_batch_id"),
        F.current_timestamp().alias("_silver_processed_at"),
        "_dq_warn_reasons",
    )
    out = silver_path("scans")
    silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)

    rejected = rejected.drop("_known_ref", "_ref_std", "_ts", "_loc_std")
    n_rej = rejected.count()
    reset_quarantine(spark, "scans")
    if n_rej:
        write_quarantine(rejected, "scans", "scan_files", batch)
    account(spark, batch, "scans", n_in, n_dups, n_rej, out)
    deduped.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
