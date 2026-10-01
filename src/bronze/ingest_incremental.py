"""Bronze ingestion, INCREMENTAL. Two different answers to 'what is new?':
  Postgres           : SOURCE watermark (updated_at > last loaded); state lives in the control table
  Scan/invoice files : a file is new if its NAME is not in Bronze yet (state is DERIVED from the target)
Bronze is APPEND-ONLY: every version of every row is kept.

Crash test:  FAIL_AFTER_WRITE=shipments python -m src.bronze.ingest_incremental
"""
import os

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.bronze.common import add_metadata, new_batch_id
from src.bronze.ingest_invoices import CARRIERS
from src.bronze.ingest_scans import SCAN_SCHEMA
from src.utils.config import PG_PROPS, PG_URL, RAW, bronze_path
from src.utils.spark import get_spark
from src.utils.state import get_ts, max_ts, set_ts

TMS_TABLES = ["customers", "carriers", "shipments", "shipment_charges", "claims"]   # locations: static


def ingest_tms(spark, batch):
    for table in TMS_TABLES:
        key = f"tms.{table}"
        wm = get_ts(spark, key)
        if wm is None:
            raise RuntimeError(f"no watermark for {key}: run python -m src.utils.bootstrap_state first")
        # The WHERE sits INSIDE the subquery, so Postgres does the filtering (and can use the updated_at
        # index). Only new or changed rows cross the wire.
        query = f"(SELECT * FROM tms.{table} WHERE updated_at > TIMESTAMP '{wm}') AS q"
        df = spark.read.jdbc(PG_URL, query, properties=PG_PROPS)
        if df.isEmpty():
            print(f"  tms.{table:<18} watermark {wm}  ->  0 new or changed rows")
            continue
        out = bronze_path(f"tms_{table}")
        add_metadata(df, "tms_postgres", f"tms.{table}", batch).write.format("delta").mode("append").save(out)

        # Derive the new watermark from what was actually LANDED, not from what we meant to land.
        landed = spark.read.format("delta").load(out).filter(F.col("_batch_id") == batch)
        n, new_wm = landed.count(), max_ts(landed, "updated_at")
        if os.getenv("FAIL_AFTER_WRITE") == table:
            # Simulated crash: Bronze HAS the rows, but the watermark was NOT advanced.
            raise RuntimeError(f"simulated crash after writing bronze tms_{table}, before advancing the watermark")
        set_ts(spark, key, new_wm)                       # advance ONLY after the write succeeded
        print(f"  tms.{table:<18} +{n:>7,} rows   watermark {wm} -> {new_wm}")


def done_files(spark, bronze_table_path):
    """File NAMES already in Bronze. The table itself is the record of what was ingested."""
    b = spark.read.format("delta").load(bronze_table_path)
    return {r["f"] for r in b.select(F.regexp_extract("_source_file", r"[^/]+$", 0).alias("f")).distinct().collect()}


def ingest_scans(spark, batch):
    out = bronze_path("scans")
    done = done_files(spark, out)          # ONE Spark job, computed once (not once per file on disk)
    new = [p for p in sorted((RAW / "scans").glob("scans_*.parquet")) if p.name not in done]
    if not new:
        print("  scans              no new files")
        return
    df = spark.read.schema(SCAN_SCHEMA).parquet(*[str(p) for p in new])
    df = (df.withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_arrival_date",
                        F.to_date(F.regexp_extract("_source_file", r"scans_(\d{8})\.parquet", 1), "yyyyMMdd")))
    add_metadata(df, "scan_files", "scans_*.parquet", batch).write.format("delta").mode("append").save(out)
    n = spark.read.format("delta").load(out).filter(F.col("_batch_id") == batch).count()
    print(f"  scans              +{n:>7,} rows from {len(new)} new files ({new[0].name} .. {new[-1].name})")


def ingest_invoices(spark, batch):
    for code, (sep, cols) in CARRIERS.items():
        out = bronze_path(f"invoices_{code.lower()}")
        done = done_files(spark, out)
        new = [p for p in sorted((RAW / "invoices" / code).glob(f"invoices_{code}_*.csv"))
               if p.name not in done]
        if not new:
            print(f"  invoices_{code.lower()}       no new files")
            continue
        schema = StructType([StructField(c, StringType()) for c in cols])
        df = (spark.read.option("header", "true").option("sep", sep).schema(schema).csv([str(p) for p in new]))
        df = df.withColumn("_source_file", F.col("_metadata.file_path"))
        add_metadata(df, f"carrier_{code.lower()}_invoices", f"invoices_{code}_*.csv", batch) \
            .write.format("delta").mode("append").save(out)
        n = spark.read.format("delta").load(out).filter(F.col("_batch_id") == batch).count()
        print(f"  invoices_{code.lower()}       +{n:>7,} rows from {len(new)} new file(s)")


def main():
    spark = get_spark("bronze_incremental")
    batch = new_batch_id("inc")
    print(f"bronze incremental run {batch}")
    ingest_tms(spark, batch)
    ingest_scans(spark, batch)
    ingest_invoices(spark, batch)
    spark.stop()


if __name__ == "__main__":
    main()
