"""Bronze ingestion: TMS (Postgres) -> Delta. Initial full load."""
from src.bronze.common import add_metadata, new_batch_id
from src.utils.config import PG_PROPS, PG_URL, bronze_path
from src.utils.spark import get_spark

# table -> numeric column used to split the read into parallel chunks (None = small, read in one go)
TABLES = {"locations": None, "customers": None, "carriers": None,
          "shipments": "shipment_id", "shipment_charges": "charge_id", "claims": None}


def read_table(spark, table, part_col):
    full = f"tms.{table}"
    if part_col is None:
        # One partition = one JDBC connection = one task. Fine for tiny tables.
        return spark.read.jdbc(PG_URL, full, properties=PG_PROPS)
    # A parallel read: Spark issues 4 queries, each with a WHERE on a slice of part_col, so 4 tasks
    # pull from Postgres at once. It needs the min and max of the column to compute the slices, so
    # we fetch those first with a tiny query that Postgres itself runs.
    bounds = spark.read.jdbc(
        PG_URL, f"(SELECT MIN({part_col}) AS lo, MAX({part_col}) AS hi FROM {full}) b",
        properties=PG_PROPS).first()
    return spark.read.jdbc(PG_URL, full, column=part_col, lowerBound=bounds["lo"],
                           upperBound=bounds["hi"], numPartitions=4, properties=PG_PROPS)


def main():
    spark = get_spark("bronze_tms")
    batch = new_batch_id("initial")
    for table, part_col in TABLES.items():
        df = add_metadata(read_table(spark, table, part_col), "tms_postgres", f"tms.{table}", batch)
        out = bronze_path(f"tms_{table}")
        # write() is the ACTION that finally runs the whole plan: JDBC read -> metadata -> Delta files.
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
        print(f"bronze tms_{table}: {spark.read.format('delta').load(out).count():,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
