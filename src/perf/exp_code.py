"""Performance lab 3: code-level choices. Usage: python -m src.perf.exp_code [udf] [collect] [cache]"""
import os
import re
import sys
from collections import Counter

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.perf.measure import report, run, start_spark
from src.quality.rules import normalise_ref
from src.silver.common import silver_path
from src.utils.config import bronze_path

PATTERN = re.compile(r"^MFL-?(\d{4})-?(\d{7})$")


@F.udf(StringType())          # a type OBJECT, not the string "string": parsing a string needs a live SparkSession
def normalise_python(ref):
    """Row-at-a-time Python UDF: every row is serialised, shipped to a Python process, processed, shipped back."""
    return None if ref is None else PATTERN.sub(r"MFL-\1-\2", ref)


def udf_vs_builtin(spark):
    refs = spark.read.format("delta").load(bronze_path("scans")).select("shipment_ref")      # 2.0M rows
    # Hash every normalised value and add them up: the SAME number if, and only if, every variant gives identical output.
    checksum = lambda expr: refs.select(F.sum(F.hash(expr)).alias("c")).first()["c"]          # noqa: E731
    checksum(F.col("shipment_ref"))                                                           # untimed warm-up
    rows = [run(spark, "A  Python UDF (row at a time)", lambda: checksum(normalise_python("shipment_ref")), repeats=1)]
    try:
        @F.pandas_udf("string")
        def normalise_pandas(s: pd.Series) -> pd.Series:
            """Vectorised UDF: rows travel in Arrow batches and are processed a whole batch at a time."""
            return s.str.replace(r"^MFL-?(\d{4})-?(\d{7})$", r"MFL-\1-\2", regex=True)

        rows.append(run(spark, "B  pandas UDF (Arrow batches)", lambda: checksum(normalise_pandas("shipment_ref"))))
    except Exception as e:      # the lab must not die because one optional variant cannot run on this machine
        print(f"pandas UDF variant skipped: {type(e).__name__}: {str(e)[:200]}")
    rows.append(run(spark, "C  built-in regexp_replace (runs inside the JVM)", lambda: checksum(normalise_ref(F.col("shipment_ref")))))
    report("UDF vs built-in function on 2.0M shipment references", rows)
    print("all variants produce identical output:", len({r["result"] for r in rows}) == 1)


def collect_vs_aggregate(spark):
    scans = spark.read.format("delta").load(silver_path("scans")).select("event_type")
    scans.count()                                                                             # untimed warm-up

    def driver_side():       # anti-pattern: pull every row to the driver (ONE process), then count in Python
        return dict(Counter(r["event_type"] for r in scans.collect()))

    def spark_side():        # the cluster does the counting; only ~10 result rows come back
        return {r["event_type"]: r["count"] for r in scans.groupBy("event_type").count().collect()}

    rows = [run(spark, "A  collect() all 1.96M rows, count in Python", driver_side, repeats=1),
            run(spark, "B  groupBy().count() in Spark, collect 10 rows", spark_side)]
    report("collect() vs aggregating in Spark", rows)
    print("same answer:", rows[0]["result"] == rows[1]["result"])


def cache_experiment(spark):
    scans = spark.read.format("delta").load(silver_path("scans")).select("shipment_ref", "event_ts", "event_type", "location_code")
    w = Window.partitionBy("shipment_ref").orderBy("event_ts")

    def enriched():          # the expensive part: a shuffle + sort + window, like the tracking fact's gap column
        return (scans.withColumn("prev", F.lag("event_ts").over(w))
                .withColumn("gap_h", (F.col("event_ts").cast("long") - F.col("prev").cast("long")) / 3600))

    def actions(df, n):      # n = how many times the SAME DataFrame is used (each use is a separate Spark job)
        out = [df.filter("gap_h IS NOT NULL").count()]   # needs gap_h, so Spark cannot prune the window away
        if n > 1:
            out.append(len(df.groupBy("location_code").agg(F.avg("gap_h")).collect()))
            out.append(len(df.groupBy("event_type").agg(F.max("gap_h")).collect()))
        return out

    def cached(n):
        def go():
            p = enriched().persist(StorageLevel.MEMORY_AND_DISK)   # computed ONCE, at the first action, then reused
            try:
                return actions(p, n)
            finally:
                p.unpersist(blocking=True)
        return go

    enriched().count()                                                                        # untimed warm-up
    rows = [run(spark, "A  NOT cached, used 1 time", lambda: actions(enriched(), 1), repeats=1),
            run(spark, "B  cached, used 1 time (cost with no benefit)", cached(1), repeats=1),
            run(spark, "C  NOT cached, used 3 times (window computed 3x)", lambda: actions(enriched(), 3), repeats=1),
            run(spark, "D  cached, used 3 times (window computed once)", cached(3), repeats=1)]
    report("Caching: worth it only when a DataFrame is reused", rows)
    print("same answers (A=B, C=D):", rows[0]["result"] == rows[1]["result"] and rows[2]["result"] == rows[3]["result"])


EXPERIMENTS = {"udf": udf_vs_builtin, "collect": collect_vs_aggregate, "cache": cache_experiment}

if __name__ == "__main__":
    spark = start_spark("perf_code")
    for name in sys.argv[1:] or list(EXPERIMENTS):
        EXPERIMENTS[name](spark)
    if os.getenv("PAUSE"):
        input("\nSpark UI is still open at http://localhost:4040. Press Enter to exit...")
    spark.stop()
