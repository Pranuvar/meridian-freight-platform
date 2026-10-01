"""Bronze ingestion, INCREMENTAL, for the reference API.
Re-fetches a LOOKBACK window before the last loaded date and APPENDS. The overlap picks up restated
values, and Silver deduplicates on (date) keeping the latest ingestion.
Usage: python -m src.bronze.ingest_api_incremental 2026-06-07     (the API server must be running)"""
import sys
from datetime import date, timedelta

from pyspark.sql import functions as F

from src.bronze.common import add_metadata, new_batch_id
from src.bronze.ingest_api import BASE, ENDPOINTS, land
from src.ingestion.api_client import fetch_all
from src.utils.config import bronze_path
from src.utils.spark import get_spark

LOOKBACK_DAYS = 7
DATE_COL = {"fx_rates": "date", "fuel_index": "week_start"}


def main():
    asof = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    spark = get_spark("bronze_api_incremental")
    batch = new_batch_id("inc")

    # Fetch everything BEFORE touching Bronze, as in Stage 7: if the API fails for good, Bronze is unchanged.
    fetched = {}
    for name, (path, _) in ENDPOINTS.items():
        bronze = spark.read.format("delta").load(bronze_path(f"api_{name}"))
        last = bronze.agg(F.max(DATE_COL[name])).first()[0]          # 'YYYY-MM-DD' strings sort correctly
        start = (date.fromisoformat(last) - timedelta(days=LOOKBACK_DAYS)).isoformat()
        print(f"fetching {name}: {start} -> {asof}  (last loaded {last}, lookback {LOOKBACK_DAYS} days)")
        fetched[name] = fetch_all(BASE + path, {"start": start, "end": asof})

    for name, (_, schema) in ENDPOINTS.items():
        if not fetched[name]:
            print(f"  {name}: nothing returned")
            continue
        file = land(name, fetched[name], batch)
        df = spark.read.schema(schema).option("multiLine", "true").json(str(file))
        df = add_metadata(df.withColumn("_source_file", F.col("_metadata.file_path")), "reference_api", name, batch)
        df.write.format("delta").mode("append").save(bronze_path(f"api_{name}"))
        print(f"  bronze api_{name}: +{len(fetched[name])} rows appended (landed {file.name})")
    spark.stop()


if __name__ == "__main__":
    main()
