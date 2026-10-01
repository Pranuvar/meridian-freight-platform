"""Pipeline state (watermarks), kept in a tiny Delta table so updates are atomic and auditable.
Two kinds of watermark live here:
  tms.<table>     SOURCE watermark: highest updated_at already pulled from Postgres
  silver.<table>  LAYER watermark : highest Bronze _ingested_at already processed into Silver
Values travel as STRINGS ('2026-05-31 23:58:12.000000') and are cast inside Spark, so the
timezone of the Python process can never shift a timestamp."""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.utils.config import CONTROL

STATE = str(CONTROL / "state")
FMT = "yyyy-MM-dd HH:mm:ss.SSSSSS"      # microsecond precision, same as Postgres


def get_ts(spark, key):
    """Watermark for `key` as a string, or None if it was never set."""
    if not DeltaTable.isDeltaTable(spark, STATE):
        return None
    row = (spark.read.format("delta").load(STATE).filter(F.col("key") == key)
           .select(F.date_format("ts", FMT).alias("s")).first())
    return row["s"] if row else None


def set_ts(spark, key, ts_str):
    """Upsert one watermark. MERGE keeps the change atomic."""
    row = (spark.createDataFrame([(key, ts_str)], "key string, s string")
           .select("key", F.col("s").cast("timestamp").alias("ts")))
    if not DeltaTable.isDeltaTable(spark, STATE):
        row.write.format("delta").mode("overwrite").save(STATE)
        return
    (DeltaTable.forPath(spark, STATE).alias("t").merge(row.alias("s"), "t.key = s.key")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def max_ts(df, column):
    """Highest value of `column` as a string (None for an empty DataFrame)."""
    return df.agg(F.date_format(F.max(column), FMT)).first()[0]
