"""SCD Type 2 for dim_customer: initial load + change detection + Delta MERGE."""
from datetime import date
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.gold.common import FAR_FUTURE, FAR_PAST, UNKNOWN_KEY, sk

TRACKED = ["segment", "account_manager", "region"]                 # Type 2: a change creates a new version
TYPE1 = ["customer_name", "payment_terms_days", "status"]          # Type 1: overwritten on ALL versions
DIM_COLS = ["customer_key", "customer_id", "customer_code", "customer_name", "segment", "account_manager",
            "region", "payment_terms_days", "status", "effective_from", "effective_to", "is_current", "row_hash"]


def _shape(df: DataFrame) -> DataFrame:
    """Turn rows that already carry effective_from / effective_to into dim_customer's final columns."""
    return df.select(
        # One key PER VERSION: same customer, different start date => different key.
        sk(F.col("customer_code"), F.col("effective_from").cast("string")).alias("customer_key"),
        "customer_id", "customer_code", "customer_name", "segment", "account_manager", "region",
        "payment_terms_days", "status", "effective_from", "effective_to",
        F.lit(True).alias("is_current"),
        # Fingerprint of the tracked columns: comparing one number finds changes without column-by-column logic.
        F.xxhash64(*TRACKED).alias("row_hash"))


def initial_load(spark: SparkSession, path: str, src: DataFrame) -> None:
    first = (src.withColumn("effective_from", F.to_date(F.lit(FAR_PAST)))
                .withColumn("effective_to", F.to_date(F.lit(FAR_FUTURE))))
    shaped = _shape(first)
    unknown = spark.createDataFrame(
        [(UNKNOWN_KEY, -1, "UNKNOWN", "Unknown customer", "Unknown", "Unknown", "Unknown", 0, "UNKNOWN",
          date(1900, 1, 1), date(9999, 12, 31), True, 0)], schema=shaped.schema)
    shaped.unionByName(unknown).write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(path)


def _type1_same(t: str, s: str):
    """TRUE when every Type 1 column is equal. eqNullSafe treats NULL = NULL as TRUE (plain = gives NULL)."""
    return reduce(lambda a, b: a & b,
                  [F.col(f"{t}.{c}").eqNullSafe(F.col(f"{s}.{c}")) for c in TYPE1])


def merge_changes(spark: SparkSession, path: str, src: DataFrame) -> dict:
    """src = ONE row per customer, current state, with an updated_at timestamp. Returns counts."""
    dim_df = DeltaTable.forPath(spark, path).toDF()
    current = dim_df.filter("is_current AND customer_id <> -1").select(
        "customer_id", F.col("row_hash").alias("cur_hash"), F.col("effective_from").alias("cur_from"))

    j = src.withColumn("row_hash", F.xxhash64(*TRACKED)).join(current, "customer_id", "left")
    is_new = F.col("cur_from").isNull()                                       # customer not in the dim yet
    is_changed = F.col("cur_from").isNotNull() & (F.col("row_hash") != F.col("cur_hash"))
    # Effective date of a change = the date the source stamped, but never before the day AFTER the
    # current version started, so a version can never have zero or negative length.
    change_from = F.greatest(F.to_date("updated_at"), F.date_add("cur_from", 1))

    versions = (j.filter(is_new | is_changed)
                 .withColumn("_is_change", is_changed)
                 .withColumn("effective_from",
                             F.when(is_new, F.to_date(F.lit(FAR_PAST))).otherwise(change_from))
                 .withColumn("effective_to", F.to_date(F.lit(FAR_FUTURE))))
    n_new = versions.filter(~F.col("_is_change")).count()
    n_changed = versions.filter(F.col("_is_change")).count()

    # Type 1 changes are counted BEFORE any merge, against all versions of each customer.
    t1 = src.select("customer_id", *TYPE1)
    n_type1 = (dim_df.filter("customer_id <> -1").alias("t").join(t1.alias("s"), "customer_id")
               .filter(~_type1_same("t", "s")).select("customer_id").distinct().count())

    if n_new + n_changed > 0:
        inserts = _shape(versions).withColumn("merge_key", F.lit(None).cast("int"))      # never match -> INSERT
        expires = (_shape(versions.filter(F.col("_is_change")))
                   .withColumn("merge_key", F.col("customer_id")))                       # match current row -> CLOSE
        staged = inserts.unionByName(expires)
        (DeltaTable.forPath(spark, path).alias("t")
            .merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = true")
            .whenMatchedUpdate(set={"is_current": F.lit(False), "effective_to": F.col("s.effective_from")})
            .whenNotMatchedInsert(values={c: F.col(f"s.{c}") for c in DIM_COLS})
            .execute())

    if n_type1 > 0:
        cond = ("t.customer_id = s.customer_id AND NOT ("
                + " AND ".join(f"t.{c} <=> s.{c}" for c in TYPE1) + ")")
        (DeltaTable.forPath(spark, path).alias("t").merge(t1.alias("s"), cond)
            .whenMatchedUpdate(set={c: F.col(f"s.{c}") for c in TYPE1}).execute())

    return {"new_customers": n_new, "new_versions": n_changed, "type1_updates": n_type1}
