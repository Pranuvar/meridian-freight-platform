"""One-off: set every watermark to what the INITIAL load already contains.
Refuses to run twice, because resetting watermarks makes the pipeline re-read old data."""
import sys

from src.utils.config import bronze_path
from src.utils.spark import get_spark
from src.utils.state import get_ts, max_ts, set_ts

TMS = ["customers", "carriers", "shipments", "shipment_charges", "claims"]   # locations has no updated_at
LAYER = {"shipments": "tms_shipments", "shipment_charges": "tms_shipment_charges", "claims": "tms_claims",
         "scans": "scans", "invoices_blz": "invoices_blz", "invoices_swf": "invoices_swf",
         "invoices_trk": "invoices_trk"}     # silver watermark name -> bronze table


def main():
    spark = get_spark("bootstrap_state")
    if get_ts(spark, "tms.shipments") is not None and "--force" not in sys.argv:
        sys.exit("state already initialised. Pass --force only if you really want to reset the watermarks.")
    bronze = lambda name: spark.read.format("delta").load(bronze_path(name))     # noqa: E731
    for t in TMS:
        wm = max_ts(bronze(f"tms_{t}"), "updated_at")
        set_ts(spark, f"tms.{t}", wm)
        print(f"  source watermark  tms.{t:<18} = {wm}")
    for name, b in LAYER.items():
        wm = max_ts(bronze(b), "_ingested_at")
        set_ts(spark, f"silver.{name}", wm)
        print(f"  layer watermark   silver.{name:<16} = {wm}")
    spark.stop()


if __name__ == "__main__":
    main()
