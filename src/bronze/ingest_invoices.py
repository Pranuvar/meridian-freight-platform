"""Bronze ingestion: carrier invoice CSVs (3 different layouts) -> 3 Delta tables."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.bronze.common import add_metadata, new_batch_id
from src.utils.config import RAW, bronze_path
from src.utils.spark import get_spark

# carrier folder -> (delimiter, our column names IN FILE ORDER).
# TruckNet's real headers contain spaces ("Invoice Number"), which Delta rejects in column names,
# so we name the columns ourselves. Spark maps schema names to CSV columns by POSITION.
CARRIERS = {
    "BLZ": (",", ["invoice_no", "invoice_date", "shipment_ref", "charge_type", "amount", "currency"]),
    "SWF": (";", ["inv_id", "inv_date", "ref", "description", "amount_inr"]),
    "TRK": (",", ["invoice_number", "inv_date", "consignment", "line_type", "net_amt", "gst_amt"]),
}


def main():
    spark = get_spark("bronze_invoices")
    batch = new_batch_id("initial")
    for code, (sep, cols) in CARRIERS.items():
        # Every column is a string. SwiftHaul's "1234,56" (decimal comma) and TruckNet's "1,234.56"
        # (thousands separator) are Silver's problem, not Bronze's.
        schema = StructType([StructField(c, StringType()) for c in cols])
        df = (spark.read.option("header", "true").option("sep", sep).schema(schema)
              .csv(str(RAW / "invoices" / code)))
        df = df.withColumn("_source_file", F.col("_metadata.file_path"))
        df = add_metadata(df, f"carrier_{code.lower()}_invoices", f"invoices_{code}_*.csv", batch)
        out = bronze_path(f"invoices_{code.lower()}")
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
        print(f"bronze invoices_{code.lower()}: {spark.read.format('delta').load(out).count():,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
