from pyspark.sql import functions as F

from src.gold.incremental import find_affected

OLD, NEW = "2026-06-01 08:00:00", "2026-06-02 08:00:00"
WM = "2026-06-01 12:00:00.000000"


def frame(spark, rows, schema):
    """Build a DataFrame and turn the _silver_processed_at string into a real timestamp."""
    return spark.createDataFrame(rows, schema).withColumn(
        "_silver_processed_at", F.col("_silver_processed_at").cast("timestamp"))


def tables(spark):
    ships = frame(spark, [(1, "A", OLD), (2, "B", OLD), (3, "C", NEW), (4, "D", OLD)],
                  "shipment_id int, shipment_ref string, _silver_processed_at string")
    charges = frame(spark, [(2, NEW), (4, OLD)], "shipment_id int, _silver_processed_at string")
    claims = frame(spark, [], "shipment_id int, _silver_processed_at string")
    scans = frame(spark, [("A", NEW), ("ZZZ", NEW)], "shipment_ref string, _silver_processed_at string")
    invoices = frame(spark, [("D", OLD)], "shipment_ref string, _silver_processed_at string")
    return ships, charges, claims, scans, invoices


def test_affected_shipments_come_from_every_source_and_must_exist(spark):
    got = {r["shipment_ref"] for r in find_affected(*tables(spark), WM).collect()}
    # C: itself is new.  B: a charge changed (found via shipment_id).  A: a late scan arrived for an OLD shipment.
    # D: only OLD rows, untouched.  ZZZ: a scan for a shipment Silver doesn't have, so it is dropped.
    assert got == {"A", "B", "C"}


def test_nothing_is_affected_when_the_watermark_is_ahead(spark):
    assert find_affected(*tables(spark), "2026-06-03 00:00:00.000000").count() == 0
