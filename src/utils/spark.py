"""One place that builds the SparkSession, so every stage uses identical settings."""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "meridian", shuffle_partitions: int = 8) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        # local[4] = run driver and executor in one JVM with 4 parallel task slots.
        .master("local[4]")
        # Driver memory. If WSL runs out of memory, lower this to 2g.
        .config("spark.driver.memory", "4g")
        # After every shuffle, Spark makes this many partitions. The default (200) is sized for
        # big clusters and creates 200 tiny tasks on a laptop, so we lower it.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Store and interpret all timestamps in UTC so nothing shifts by a timezone offset
        # between Postgres, Spark and Delta.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        # These two lines switch on Delta Lake support (MERGE, time travel, transaction log).
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    # configure_spark_with_delta_pip adds the matching Delta jar. extra_packages adds the Postgres
    # JDBC driver, which Spark needs to talk to Postgres. Both are downloaded from Maven on the
    # first run (about a minute) and cached after that.
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["org.postgresql:postgresql:42.7.4"]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
