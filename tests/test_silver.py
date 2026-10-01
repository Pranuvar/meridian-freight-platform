from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from src.silver.build_invoices import parse_blz, parse_swf, parse_trk
from src.silver.common import dedupe


def _meta(df):
    return df.withColumn("_source_file", F.lit("f.csv")).withColumn("_ingested_at", F.current_timestamp())


def test_three_carrier_layouts_parse_to_the_same_values(spark):
    """The SAME charge, written three different ways, must come out identical."""
    blz = _meta(spark.createDataFrame(
        [("BLZ-20250115", "2025-01-15", "MFL-2025-0000001", "Freight", "12345.67", "INR")],
        "invoice_no string, invoice_date string, shipment_ref string, charge_type string, amount string, currency string"))
    swf = _meta(spark.createDataFrame(
        [("SWF-20250115", "15-01-2025", "MFL20250000001", "Base freight", "12345,67")],
        "inv_id string, inv_date string, ref string, description string, amount_inr string"))
    trk = _meta(spark.createDataFrame(
        [("TRK/20250115", "15-Jan-25", "MFL-2025-0000001", "F", "12,345.67", "2,222.22")],
        "invoice_number string, inv_date string, consignment string, line_type string, net_amt string, gst_amt string"))
    rows = [parse_blz(blz).first(), parse_swf(swf).first(), parse_trk(trk).first()]
    for r in rows:
        assert r["invoice_date"] == date(2025, 1, 15)
        assert r["charge_type"] == "FREIGHT"
        assert r["amount"] == Decimal("12345.67")
    assert rows[2]["gst_amount"] == Decimal("2222.22")


def test_dedupe_keeps_the_latest_version(spark):
    df = spark.createDataFrame([(1, "old", 1), (1, "new", 2), (2, "only", 1)], "id int, val string, v int")
    out = {r["id"]: r["val"] for r in dedupe(df, ["id"], [F.col("v").desc()]).collect()}
    assert out == {1: "new", 2: "only"}
