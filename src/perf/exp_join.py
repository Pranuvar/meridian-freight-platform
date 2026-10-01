"""Performance lab 1: join strategy, shuffle partitions, coalesce vs repartition.
Usage: python -m src.perf.exp_join [join] [shuffle] [coalesce]      (no argument = all three)
       PAUSE=1 python -m src.perf.exp_join   keeps the Spark UI open at the end (http://localhost:4040)"""
import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.perf.measure import conf, noop, report, run, show_plan, start_spark
from src.silver.common import silver_path


def silver(spark, name):
    return spark.read.format("delta").load(silver_path(name))


def sortmerge_vs_broadcast(spark):
    scans = silver(spark, "scans").select("shipment_ref", "event_type")              # 1.96M rows
    ships = silver(spark, "shipments").select("shipment_ref", "customer_id")        # 202k rows
    # New DataFrame objects on every call, so each run is planned fresh under whatever settings are active.
    plain = lambda: scans.join(ships, "shipment_ref")                              # noqa: E731  Spark chooses
    hinted = lambda: scans.join(F.broadcast(ships), "shipment_ref")                # noqa: E731  we tell it
    noop(plain())        # untimed warm-up: fills the file cache and warms the JVM, so run A isn't unfairly slow
    rows = []
    # Adaptive execution (AQE) re-plans joins while the query runs, which would blur the comparison. For A and B we
    # switch it off, so the plan printed here is exactly the plan that executes.
    with conf(spark, {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": "-1"}):
        show_plan("A", plain(), "SortMergeJoin", "Exchange hashpartitioning", r"Sort \[", "BroadcastHashJoin")
        rows.append(run(spark, "A  sort-merge join (both sides shuffled + sorted)", lambda: noop(plain())))
    with conf(spark, {"spark.sql.adaptive.enabled": "false"}):
        show_plan("B", hinted(), "SortMergeJoin", "Exchange hashpartitioning", "BroadcastExchange", "BroadcastHashJoin")
        rows.append(run(spark, "B  broadcast join (small side sent to every task)", lambda: noop(hinted())))
    rows.append(run(spark, "C  Spark defaults (adaptive execution ON)", lambda: noop(plain())))
    report("Join: 1.96M scans x 202k shipments", rows)


def shuffle_partitions(spark):
    scans = silver(spark, "scans").select("shipment_ref")
    agg = lambda: scans.groupBy("shipment_ref").agg(F.count("*").alias("n"))       # noqa: E731  ~200k groups: a real shuffle
    noop(agg())
    cases = [("200 partitions, adaptive OFF (the Spark default)", "200", "false"),
             ("8 partitions, adaptive OFF (tuned for 4 cores)", "8", "false"),
             ("1 partition, adaptive OFF (no parallelism at all)", "1", "false"),
             ("200 partitions, adaptive ON (Spark merges small ones)", "200", "true")]
    rows = []
    for label, parts, aqe in cases:
        with conf(spark, {"spark.sql.shuffle.partitions": parts, "spark.sql.adaptive.enabled": aqe}):
            rows.append(run(spark, label, lambda: noop(agg())))
    report("Shuffle partitions: groupBy over 1.96M scans (compare the TASKS column)", rows)


def coalesce_vs_repartition(spark):
    scans = silver(spark, "scans").select("shipment_ref", "event_ts", "event_type")
    w = Window.partitionBy("shipment_ref").orderBy("event_ts")
    # A heavy step upstream of the partitioning decision: a shuffle, a sort and a window function.
    heavy = lambda: scans.withColumn("prev_ts", F.lag("event_ts").over(w))        # noqa: E731
    noop(heavy())
    rows = []
    with conf(spark, {"spark.sql.adaptive.enabled": "false"}):
        show_plan("coalesce(1)", heavy().coalesce(1), "Coalesce", "Exchange hashpartitioning", "Window")
        rows.append(run(spark, "coalesce(1): merge partitions, no extra shuffle", lambda: noop(heavy().coalesce(1))))
        show_plan("repartition(1)", heavy().repartition(1), "Exchange SinglePartition", "Exchange hashpartitioning", "Window")
        rows.append(run(spark, "repartition(1): one more shuffle, upstream stays parallel", lambda: noop(heavy().repartition(1))))
    report("coalesce vs repartition with heavy work upstream", rows)


EXPERIMENTS = {"join": sortmerge_vs_broadcast, "shuffle": shuffle_partitions, "coalesce": coalesce_vs_repartition}

if __name__ == "__main__":
    spark = start_spark("perf_join")
    for name in sys.argv[1:] or list(EXPERIMENTS):
        EXPERIMENTS[name](spark)
    if os.getenv("PAUSE"):
        input("\nSpark UI is still open at http://localhost:4040 (SQL tab = plans, Stages tab = tasks). Press Enter to exit...")
    spark.stop()
