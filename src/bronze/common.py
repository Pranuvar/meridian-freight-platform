"""Helpers shared by all Bronze ingestion jobs."""
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def new_batch_id(prefix: str) -> str:
    """A label for one pipeline run, e.g. initial_20260930T141500. Stored on every row it loads,
    so a bad batch can be found (and later removed) precisely."""
    return f"{prefix}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"


def add_metadata(df: DataFrame, source_system: str, source_object: str, batch_id: str) -> DataFrame:
    """Lineage columns. Prefixed with _ so they are never confused with business columns."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_object", F.lit(source_object))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_processing_date", F.current_date())
    )
