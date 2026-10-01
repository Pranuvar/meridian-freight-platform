"""Bronze ingestion: reference API -> raw JSON files (landing) -> Delta.
Usage: python -m src.bronze.ingest_api [START_DATE] [END_DATE]   (defaults: full history)"""
import json
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.bronze.common import add_metadata, new_batch_id
from src.ingestion.api_client import fetch_all
from src.utils.config import RAW, bronze_path
from src.utils.spark import get_spark

BASE = "http://localhost:8000"
ENDPOINTS = {  # name -> (path, explicit schema). Dates stay strings in Bronze; Silver parses them.
    "fx_rates": ("/fx-rates", StructType([StructField("date", StringType()), StructField("base", StringType()),
                                          StructField("quote", StringType()), StructField("rate", DoubleType())])),
    "fuel_index": ("/fuel-index", StructType([StructField("week_start", StringType()),
                                              StructField("diesel_inr_per_litre", DoubleType())])),
}


def land(name, records, batch):
    """Write the raw response to disk ATOMICALLY: temp file, then rename. A crash never leaves a half file."""
    folder = RAW / "api" / name
    folder.mkdir(parents=True, exist_ok=True)
    final = folder / f"{name}_{batch}.json"
    tmp = final.with_suffix(".tmp")
    tmp.write_text(json.dumps(records))
    os.replace(tmp, final)
    return final


def main():
    start = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    end = sys.argv[2] if len(sys.argv) > 2 else "2026-05-31"
    spark = get_spark("bronze_api")
    batch = new_batch_id("initial")

    # STEP 1: fetch EVERYTHING before touching Bronze. If the API fails for good, we stop here
    # (ApiError, non-zero exit) and Bronze still holds the last good data.
    fetched = {}
    for name, (path, _) in ENDPOINTS.items():
        print(f"fetching {name} {start} -> {end}")
        fetched[name] = fetch_all(BASE + path, {"start": start, "end": end})

    # STEP 2: land the raw files, then load Bronze from those files (not from memory).
    for name, (_, schema) in ENDPOINTS.items():
        file = land(name, fetched[name], batch)
        df = spark.read.schema(schema).option("multiLine", "true").json(str(file))
        df = df.withColumn("_source_file", F.col("_metadata.file_path"))
        df = add_metadata(df, "reference_api", name, batch)
        out = bronze_path(f"api_{name}")
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
        print(f"bronze api_{name}: {spark.read.format('delta').load(out).count():,} rows (landed {file.name})")
    spark.stop()


if __name__ == "__main__":
    main()
