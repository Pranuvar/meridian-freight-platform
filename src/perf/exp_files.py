"""Performance lab 2: file counts, file layout, partition pruning, column pruning.
Usage: python -m src.perf.exp_files [health] [files]       (no argument = both; `files` takes a few minutes)"""
import os
import shutil
import sys
import time

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.perf.measure import conf, report, run, scan_filters, start_spark
from src.silver.common import silver_path
from src.utils.config import DATA, RAW

PERF = DATA / "perf"
MAY = ("2026-05-01", "2026-05-31")


def detail(spark, path):
    """(number of active files, size in bytes) of a Delta table, from its transaction log."""
    d = DeltaTable.forPath(spark, str(path)).detail().select("numFiles", "sizeInBytes").first()
    return d["numFiles"], d["sizeInBytes"]


def table_health(spark):
    """File count and average file size for every table in the platform. Many files of a few KB = small-file problem."""
    rows = []
    for layer in ("bronze", "silver", "gold"):
        for p in sorted((DATA / layer).iterdir()):
            if p.is_dir() and DeltaTable.isDeltaTable(spark, str(p)):
                n, size = detail(spark, p)
                rows.append((f"{layer}/{p.name}", n, size))
    raw = list((RAW / "scans").glob("*.parquet"))
    rows.append(("raw/scans (landing folder)", len(raw), sum(f.stat().st_size for f in raw)))
    print(f"\n{'table':<44}{'files':>8}{'size MB':>10}{'avg file KB':>13}")
    for name, n, size in sorted(rows, key=lambda r: -r[1]):
        print(f"{name:<44}{n:>8,}{size / 1e6:>10.1f}{size / max(n, 1) / 1e3:>13,.0f}")


def files_and_layout(spark):
    shutil.rmtree(PERF, ignore_errors=True)            # start clean: earlier runs leave old file versions on disk
    read = lambda p: spark.read.format("delta").load(str(p))                          # noqa: E731
    src = read(silver_path("scans")).withColumn("event_month", F.date_format("event_date", "yyyy-MM"))
    small, month, day = PERF / "scans_small", PERF / "scans_by_month", PERF / "scans_by_day"

    print("writing three copies of the Silver scans table with different file layouts (a few minutes)...")
    # 1) 400 tiny files: what a job produces when it writes with far too many partitions.
    src.drop("event_month").repartition(400).write.format("delta").save(str(small))
    # 2) partitioned by MONTH (17 folders). repartition(8, "event_month") FIRST puts every row of a month into ONE
    #    task, so each folder gets ONE file. Without it, every task would write a file into every folder.
    src.repartition(8, "event_month").write.format("delta").partitionBy("event_month").save(str(month))
    # 3) partitioned by DAY (~520 folders): the classic over-partitioning mistake.
    src.drop("event_month").repartition(8, "event_date").write.format("delta").partitionBy("event_date").save(str(day))

    by_location = lambda df: {r["location_code"]: r["count"] for r in df.groupBy("location_code").count().collect()}  # noqa: E731
    label = lambda text, p: f"{text} ({detail(spark, p)[0]} files, {detail(spark, p)[1] / 1e6:.0f} MB)"              # noqa: E731
    whole = lambda p: (lambda: by_location(read(p)))                                                                  # noqa: E731

    # ---- part 1: small files and OPTIMIZE
    read(small).count()                                                     # untimed warm-up
    rows = [run(spark, label("A  unpartitioned, too many small files", small), whole(small))]
    # Default OPTIMIZE merges files up to 1 GB, which would leave this 50 MB table as ONE file = one task = no
    # parallelism. A 16 MB target gives a handful of files, so all 4 cores stay busy. (Production tables use ~128 MB-1 GB.)
    with conf(spark, {"spark.databricks.delta.optimize.maxFileSize": str(16 * 1024 * 1024)}):
        t0 = time.perf_counter()
        DeltaTable.forPath(spark, str(small)).optimize().executeCompaction()
        print(f"OPTIMIZE compacted the small files in {time.perf_counter() - t0:.1f}s (a one-off cost)")
    rows.append(run(spark, label("B  same table after OPTIMIZE", small), whole(small)))
    report("Small files: full scan of the scans table", rows)

    # ---- part 2: three layouts, two queries. After OPTIMIZE the compacted table is our "flat" layout.
    flat = small
    names = {"flat": ("flat, compacted", flat), "month": ("partitioned by month", month), "day": ("partitioned by day", day)}
    one_month = {"flat": lambda: by_location(read(flat).filter(F.col("event_date").between(*MAY))),
                 "month": lambda: by_location(read(month).filter(F.col("event_month") == "2026-05")),
                 "day": lambda: by_location(read(day).filter(F.col("event_date").between(*MAY)))}
    everything = {"flat": whole(flat), "month": whole(month), "day": whole(day)}
    for title, queries in (("ONE month of data (May 2026)", one_month), ("the WHOLE table", everything)):
        rows = [run(spark, label(names[k][0], names[k][1]), q) for k, q in queries.items()]
        report(f"Layout comparison, query for {title}", rows)
        print("every layout returns the same answer:", all(r["result"] == rows[0]["result"] for r in rows))

    print("\nwhat Spark pushes into the file scan for the one-month query:")
    for k, df in (("flat", read(flat).filter(F.col("event_date").between(*MAY))),
                  ("month", read(month).filter(F.col("event_month") == "2026-05")),
                  ("day", read(day).filter(F.col("event_date").between(*MAY)))):
        print(f"  {k:<6}{scan_filters(df)}")

    # ---- part 3: column pruning
    def one_column():
        return read(flat).agg(F.sum(F.hash("location_code"))).first()[0]

    def all_columns():
        d = read(flat)
        return d.agg(F.sum(F.hash(*d.columns))).first()[0]      # forces every column to be read

    report("Column pruning on the flat table (compare the INPUT MB column)",
           [run(spark, "A  touch ALL columns (the SELECT * habit)", all_columns),
            run(spark, "B  touch ONE column", one_column)])


EXPERIMENTS = {"health": table_health, "files": files_and_layout}

if __name__ == "__main__":
    spark = start_spark("perf_files")
    for name in sys.argv[1:] or list(EXPERIMENTS):
        EXPERIMENTS[name](spark)
    if os.getenv("PAUSE"):
        input("\nSpark UI is still open at http://localhost:4040. Press Enter to exit...")
    spark.stop()
