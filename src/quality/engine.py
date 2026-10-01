"""The reusable quality engine: tag rows, split clean from rejected, measure, quarantine, log."""
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField, StructType,
                               TimestampType)

from src.quality.rules import Rule
from src.utils.config import CONTROL, QUARANTINE


def evaluate(df: DataFrame, rules: list) -> DataFrame:
    """Add two array columns listing which rules each row broke. Lazy: nothing runs yet."""
    def tag(severity: str):
        cols = [F.when(r.fail, F.lit(r.name)) for r in rules if r.severity == severity]
        # F.when(...) is NULL when the rule passes. The leading NULL string keeps the array non-empty
        # and correctly typed even if a table has no rules of that severity.
        arr = F.array(F.lit(None).cast("string"), *cols)
        return F.filter(arr, lambda x: x.isNotNull())      # drop the NULLs, keep only names that fired

    return (df.withColumn("_dq_reject_reasons", tag("reject"))
              .withColumn("_dq_warn_reasons", tag("warn")))


def split(df: DataFrame, rules: list):
    """Return (clean, rejected). Warnings ride along on clean rows so Gold can expose them."""
    tagged = evaluate(df, rules)
    is_bad = F.size("_dq_reject_reasons") > 0
    clean = tagged.filter(~is_bad).drop("_dq_reject_reasons")
    rejected = tagged.filter(is_bad)
    return clean, rejected


def measure(df: DataFrame, rules: list):
    """One pass over the data: rows in, rows rejected, rows with warnings, and a count per rule.
    Returns (records_in, rejected, warned, [(rule_name, severity, failed_count), ...])."""
    tagged = evaluate(df, rules)
    aggs = [F.count(F.lit(1)).alias("_total"),
            F.sum(F.when(F.size("_dq_reject_reasons") > 0, 1).otherwise(0)).alias("_rejected"),
            F.sum(F.when(F.size("_dq_warn_reasons") > 0, 1).otherwise(0)).alias("_warned")]
    aggs += [F.sum(F.when(r.fail, 1).otherwise(0)).alias(r.name) for r in rules]
    row = tagged.agg(*aggs).first().asDict()
    results = [(r.name, r.severity, int(row[r.name] or 0)) for r in rules]   # sum() of nothing is NULL
    return int(row["_total"]), int(row["_rejected"] or 0), int(row["_warned"] or 0), results


def write_quarantine(rejected: DataFrame, table: str, source: str, batch_id: str) -> None:
    """Store rejected rows with their reasons. The original row is kept as JSON so ONE quarantine
    table can hold rows from tables with different schemas."""
    cols = [c for c in rejected.columns if not c.startswith("_dq_")]
    q = rejected.select(
        F.lit(table).alias("table_name"),
        F.lit(source).alias("source_system"),
        F.lit(batch_id).alias("batch_id"),
        F.current_timestamp().alias("quarantined_at"),
        F.concat_ws("|", "_dq_reject_reasons").alias("reject_reasons"),
        F.to_json(F.struct(*[F.col(c) for c in cols])).alias("record"),
    )
    q.write.format("delta").mode("append").save(str(QUARANTINE / "rejected_records"))


LOG_SCHEMA = StructType([
    StructField("run_ts", TimestampType()), StructField("batch_id", StringType()),
    StructField("table_name", StringType()), StructField("source_system", StringType()),
    StructField("rule_name", StringType()), StructField("severity", StringType()),
    StructField("failed_count", LongType()), StructField("records_in", LongType()),
    StructField("failed_pct", DoubleType()),
])


def log_metrics(spark: SparkSession, table: str, source: str, batch_id: str,
                records_in: int, results: list) -> None:
    """Append one row per (table, rule) to the quality log. Long format on purpose: it is easy to
    chart in Power BI later ("failures per rule per day")."""
    now = datetime.now(timezone.utc)
    rows = [(now, batch_id, table, source, name, sev, int(cnt), int(records_in),
             round(cnt / records_in * 100, 4) if records_in else 0.0) for name, sev, cnt in results]
    if rows:
        spark.createDataFrame(rows, LOG_SCHEMA).write.format("delta").mode("append") \
            .save(str(CONTROL / "dq_run_log"))


def reset_quarantine(spark: SparkSession, table: str) -> None:
    """Delete earlier quarantine rows for this table so re-running a job doesn't duplicate them.
    (Stage 8 replaces this with batch-scoped handling for incremental loads.)"""
    from delta.tables import DeltaTable
    path = str(QUARANTINE / "rejected_records")
    if DeltaTable.isDeltaTable(spark, path):
        DeltaTable.forPath(spark, path).delete(f"table_name = '{table}'")
