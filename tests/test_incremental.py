from pyspark.sql import functions as F

from src.silver.incremental import insert_only_merge, upsert_merge


def _with_ts(df):
    return df.withColumn("updated_at", F.col("updated_at").cast("timestamp"))


def test_upsert_takes_only_strictly_newer_versions_and_is_replay_safe(spark, tmp_path):
    path = str(tmp_path / "t")
    _with_ts(spark.createDataFrame([(1, "old", "2026-06-01 10:00:00"), (2, "keep", "2026-06-01 10:00:00")],
                                   "id int, val string, updated_at string")).write.format("delta").save(path)
    src = _with_ts(spark.createDataFrame(
        [(1, "new", "2026-06-02 10:00:00"), (2, "stale", "2026-05-31 10:00:00"), (3, "fresh", "2026-06-02 10:00:00")],
        "id int, val string, updated_at string"))
    read = lambda: {r["id"]: r["val"] for r in spark.read.format("delta").load(path).collect()}   # noqa: E731
    upsert_merge(spark, path, src, "id")
    assert read() == {1: "new", 2: "keep", 3: "fresh"}     # newer updated, older ignored, new inserted
    upsert_merge(spark, path, src, "id")                   # replaying the same data must change nothing
    assert read() == {1: "new", 2: "keep", 3: "fresh"}


def test_insert_only_merge_ignores_existing_rows_and_matches_nulls(spark, tmp_path):
    path = str(tmp_path / "t")
    schema = "a int, b string"
    spark.createDataFrame([(1, "x"), (2, None)], schema).write.format("delta").save(path)
    batch = spark.createDataFrame([(1, "x"), (2, None), (3, "y")], schema)
    insert_only_merge(spark, path, batch, ["a", "b"])
    # only (3, "y") is new. (2, NULL) matched because <=> treats NULL = NULL as true.
    assert spark.read.format("delta").load(path).count() == 3
    insert_only_merge(spark, path, batch, ["a", "b"])
    assert spark.read.format("delta").load(path).count() == 3
