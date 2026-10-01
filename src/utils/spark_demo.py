"""Learning script: lazy evaluation, narrow vs wide, the plan, and the Spark UI."""
from pyspark.sql import functions as F

from src.utils.config import RAW
from src.utils.spark import get_spark

spark = get_spark("spark_demo")

# Reading is lazy, apart from a small job to list files and read the schema.
df = spark.read.option("mergeSchema", "true").parquet(str(RAW / "scans"))
print("input partitions:", df.rdd.getNumPartitions())

# Both lines below only BUILD a plan. Nothing is computed yet.
delivered = df.filter(F.col("event_type") == "DELIVERED")      # narrow
by_loc = delivered.groupBy("location_code").count()            # wide -> shuffle

print("\n--- physical plan (read it bottom to top) ---")
by_loc.explain()

print("\n--- show() is an ACTION: this is where the job actually runs ---")
by_loc.orderBy(F.desc("count")).show(25)

print("total scan rows:", df.count())                          # a second action = a second job

input("\nOpen http://localhost:4040 (Jobs and Stages tabs), then press Enter to exit...")
spark.stop()
