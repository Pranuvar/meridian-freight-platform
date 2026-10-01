"""Content fingerprint of each Gold fact: a hash of every business column, summed. Bookkeeping columns
(_gold_*) are ignored, so a re-run that rewrites identical data gives the SAME fingerprint."""
from pyspark.sql import functions as F

from src.gold.common import gold_path
from src.utils.spark import get_spark

spark = get_spark("fingerprint")
for name in ["fact_shipment", "fact_tracking_event", "fact_carrier_invoice_line"]:
    t = spark.read.format("delta").load(gold_path(name))
    cols = [c for c in t.columns if not c.startswith("_gold")]
    # cast to decimal(38,0) so the sum is exact and cannot overflow a 64-bit integer
    row = t.agg(F.count("*").alias("n"), F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("fp")).first()
    print(f"{name:<28} rows={row['n']:>10,}  fingerprint={row['fp']}")
spark.stop()
