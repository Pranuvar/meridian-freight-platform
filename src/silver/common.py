"""Helpers shared by the Silver jobs: silver paths, dedupe, and the row-accounting check."""
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.utils.config import CONTROL, SILVER


def silver_path(name: str) -> str:
    return str(SILVER / name)


def dedupe(df: DataFrame, keys: list, order_by: list) -> DataFrame:
    """Keep ONE row per key: the first row after sorting by order_by.
    row_number() over a window is a WIDE transformation (shuffle by key, sort inside each partition)."""
    w = Window.partitionBy(*keys).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


RUN_LOG_SCHEMA = StructType([
    StructField("run_ts", TimestampType()), StructField("batch_id", StringType()),
    StructField("layer", StringType()), StructField("table_name", StringType()),
    StructField("records_in", LongType()), StructField("records_out", LongType()),
    StructField("records_rejected", LongType()), StructField("duplicates_removed", LongType()),
])


def account(spark: SparkSession, batch: str, table: str, n_in: int, n_dups: int, n_rej: int,
            out_path: str) -> bool:
    """The core Silver guarantee: in = out + rejected + duplicates. Prints it, logs it, returns pass/fail."""
    n_out = spark.read.format("delta").load(out_path).count()
    ok = n_in == n_out + n_rej + n_dups
    print(f"silver {table:<18} in={n_in:>10,} = out={n_out:>10,} + rejected={n_rej:>8,} "
          f"+ duplicates={n_dups:>8,}  {'PASS' if ok else 'FAIL'}")
    row = [(datetime.now(timezone.utc), batch, "silver", table, int(n_in), int(n_out), int(n_rej), int(n_dups))]
    spark.createDataFrame(row, RUN_LOG_SCHEMA).write.format("delta").mode("append").save(str(CONTROL / "run_log"))
    return ok
