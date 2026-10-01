"""Silver: three carrier invoice layouts -> ONE clean schema. All the per-carrier mess
(delimiters, date formats, decimal commas, thousands separators, charge-type codes) is contained here."""
from pyspark.sql import functions as F

from src.bronze.common import new_batch_id
from src.quality.engine import reset_quarantine, split, write_quarantine
from src.quality.rules import normalise_ref
from src.quality.rules_silver import invoice_rules
from src.silver.common import account, dedupe, silver_path
from src.utils.config import bronze_path
from src.utils.spark import get_spark

MONEY = "decimal(12,2)"
META = ["_source_file", "_ingested_at"]


def map_col(col, mapping: dict):
    """CASE WHEN col = k1 THEN v1 WHEN ... ; unmapped values become NULL (caught by a rule)."""
    expr = None
    for k, v in mapping.items():
        expr = F.when(col == k, v) if expr is None else expr.when(col == k, v)
    return expr


def parse_blz(df):
    """BlueLine: comma CSV, ISO dates, dashed refs, plain decimals."""
    return df.select(
        F.lit("BLZ").alias("carrier_code"), "invoice_no",
        F.to_date("invoice_date", "yyyy-MM-dd").alias("invoice_date"),
        F.col("shipment_ref").alias("_ref_raw"),
        F.upper(F.trim("charge_type")).alias("charge_type"),
        F.col("amount").cast(MONEY).alias("amount"),
        F.lit(None).cast(MONEY).alias("gst_amount"),
        F.upper(F.trim("currency")).alias("currency"), *META)


def parse_swf(df):
    """SwiftHaul: dd-MM-yyyy dates, refs without dashes, DECIMAL COMMA ("12345,67")."""
    desc = F.upper(F.trim("description"))
    return df.select(
        F.lit("SWF").alias("carrier_code"), F.col("inv_id").alias("invoice_no"),
        F.to_date("inv_date", "dd-MM-yyyy").alias("invoice_date"),
        F.col("ref").alias("_ref_raw"),
        map_col(desc, {"BASE FREIGHT": "FREIGHT", "FUEL SURCHARGE": "FUEL", "DETENTION": "DETENTION"}).alias("charge_type"),
        F.regexp_replace("amount_inr", ",", ".").cast(MONEY).alias("amount"),
        F.lit(None).cast(MONEY).alias("gst_amount"),
        F.lit("INR").alias("currency"), *META)


def parse_trk(df):
    """TruckNet: dd-MMM-yy dates, line-type codes, THOUSANDS separators ("12,345.67"), NET and GST split."""
    lt = F.upper(F.trim("line_type"))
    return df.select(
        F.lit("TRK").alias("carrier_code"), F.col("invoice_number").alias("invoice_no"),
        F.to_date("inv_date", "dd-MMM-yy").alias("invoice_date"),
        F.col("consignment").alias("_ref_raw"),
        map_col(lt, {"F": "FREIGHT", "FS": "FUEL", "D": "DETENTION"}).alias("charge_type"),
        F.regexp_replace("net_amt", ",", "").cast(MONEY).alias("amount"),
        F.regexp_replace("gst_amt", ",", "").cast(MONEY).alias("gst_amount"),
        F.lit("INR").alias("currency"), *META)


def main():
    spark = get_spark("silver_invoices")
    batch = new_batch_id("initial")

    def b(name):
        return spark.read.format("delta").load(bronze_path(name))

    unified = (parse_blz(b("invoices_blz"))
               .unionByName(parse_swf(b("invoices_swf")))
               .unionByName(parse_trk(b("invoices_trk"))))
    n_in = unified.count()

    known = (b("tms_shipments").select(F.col("shipment_ref").alias("_ref_std")).distinct()
             .withColumn("_known_ref", F.lit(1)))
    prepared = unified.withColumn("_ref_std", normalise_ref(F.col("_ref_raw")))

    # A duplicate = same carrier, invoice, shipment and charge type. Earliest ingestion wins.
    deduped = dedupe(prepared, ["carrier_code", "invoice_no", "_ref_std", "charge_type"],
                     [F.col("_ingested_at").asc(), F.col("_source_file").asc()]).persist()
    n_dups = n_in - deduped.count()

    joined = deduped.join(F.broadcast(known), "_ref_std", "left")
    clean, rejected = split(joined, invoice_rules())

    silver = clean.select(
        "carrier_code", "invoice_no", "invoice_date",
        F.col("_ref_std").alias("shipment_ref"),
        "charge_type", "amount", "gst_amount", "currency", "_source_file",
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.lit(batch).alias("_silver_batch_id"),
        F.current_timestamp().alias("_silver_processed_at"),
        "_dq_warn_reasons")
    out = silver_path("carrier_invoice_lines")
    silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)

    rejected = rejected.drop("_known_ref", "_ref_std")
    n_rej = rejected.count()
    reset_quarantine(spark, "carrier_invoice_lines")
    if n_rej:
        write_quarantine(rejected, "carrier_invoice_lines", "carrier_invoices", batch)
    account(spark, batch, "carrier_invoice_lines", n_in, n_dups, n_rej, out)
    deduped.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
