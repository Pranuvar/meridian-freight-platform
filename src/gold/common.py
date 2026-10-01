"""Helpers shared by the Gold jobs: paths, surrogate keys, unknown-member key, run logging."""
from datetime import datetime, timezone

from pyspark.sql import Column
from pyspark.sql import functions as F

from src.silver.common import RUN_LOG_SCHEMA
from src.utils.config import CONTROL, GOLD

UNKNOWN_KEY = -1            # every dimension (except dim_date) has a row with this key
FAR_PAST = "1900-01-01"     # start of the FIRST version of an SCD2 row
FAR_FUTURE = "9999-12-31"   # end of the CURRENT version of an SCD2 row


def gold_path(name: str) -> str:
    return str(GOLD / name)


def sk(*cols) -> Column:
    """Deterministic 64-bit surrogate key from natural-key columns. The same natural key always
    gives the same surrogate, so rebuilds and reruns never change keys already used by facts."""
    return F.xxhash64(*cols)


def log_gold(spark, batch: str, table: str, n_in: int, n_out: int) -> None:
    row = [(datetime.now(timezone.utc), batch, "gold", table, int(n_in), int(n_out), 0, 0)]
    spark.createDataFrame(row, RUN_LOG_SCHEMA).write.format("delta").mode("append") \
        .save(str(CONTROL / "run_log"))
