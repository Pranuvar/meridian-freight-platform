"""Bronze ingestion: daily tracking-scan Parquet files -> Delta."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.bronze.common import add_metadata, new_batch_id
from src.utils.config import RAW, bronze_path
from src.utils.spark import get_spark

# Explicit schema = the contract with the scanning system. Everything is a string on purpose:
# event_ts arrives in three formats, so Bronze must not try to interpret it.
SCAN_SCHEMA = StructType([
    StructField("shipment_ref", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("location_code", StringType()),
    StructField("scan_source", StringType()),   # missing in the early files -> read as null
])


def main():
    spark = get_spark("bronze_scans")
    batch = new_batch_id("initial")

    df = spark.read.schema(SCAN_SCHEMA).parquet(str(RAW / "scans"))
    df = (
        # _metadata is a hidden column Spark exposes for file sources (Spark 3.3+); it tells us which
        # file each row came from.
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        # File names are scans_YYYYMMDD.parquet, so the ARRIVAL date lives in the file name.
        .withColumn("_arrival_date",
                    F.to_date(F.regexp_extract("_source_file", r"scans_(\d{8})\.parquet", 1), "yyyyMMdd"))
    )
    df = add_metadata(df, "scan_files", "scans_*.parquet", batch)

    out = bronze_path("scans")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
    print(f"bronze scans: {spark.read.format('delta').load(out).count():,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
