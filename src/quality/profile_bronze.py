"""Measure-only data-quality profile of Bronze. Nothing is modified or quarantined here."""
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.quality.engine import log_metrics, measure
from src.quality.rules import customer_rules, normalise_ref, scan_rules, shipment_rules
from src.utils.config import bronze_path
from src.utils.spark import get_spark

spark = get_spark("bronze_profile")
batch = f"profile_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"


def bronze(name):
    return spark.read.format("delta").load(bronze_path(name))


# collect() pulls data onto the driver. It is safe here ONLY because the result is 18 rows.
valid_locations = [r["location_code"] for r in bronze("tms_locations").select("location_code").collect()]

shipments, customers, scans = bronze("tms_shipments"), bronze("tms_customers"), bronze("scans")
# Every shipment reference we know about, used for referential-integrity (orphan) checks.
ship_refs = shipments.select(F.col("shipment_ref").alias("ref")).distinct()


def report(table, source, df, rules, extra=None):
    n, rejected, warned, results = measure(df, rules)
    results = results + (extra or [])
    log_metrics(spark, table, source, batch, n, results)
    print(f"\n=== {table}: {n:,} rows | would reject {rejected:,} | with warnings {warned:,} ===")
    for name, sev, cnt in results:
        print(f"  {name:<28}{sev:<8}{cnt:>10,}  {cnt / n * 100:6.2f}%")


report("tms_shipments", "tms_postgres", shipments, shipment_rules())
report("tms_customers", "tms_postgres", customers, customer_rules())

# Duplicates: rows minus distinct keys. groupBy is a WIDE transformation, so this triggers a shuffle.
scan_key = ["shipment_ref", "event_type", "event_ts", "location_code"]
scan_dups = scans.groupBy(*scan_key).count().agg(F.sum(F.col("count") - 1)).first()[0] or 0
# Orphans: normalise the ref, then a left_anti join keeps scans with NO matching shipment.
scan_orphans = (scans.withColumn("ref", normalise_ref(F.col("shipment_ref")))
                .join(ship_refs, "ref", "left_anti").count())
report("scans", "scan_files", scans, scan_rules(valid_locations),
       extra=[("duplicate_rows", "info", int(scan_dups)), ("orphan_no_shipment", "info", int(scan_orphans))])

# Each carrier has its own layout: (column holding the shipment ref, columns that identify a line).
INVOICES = {
    "invoices_blz": ("shipment_ref", ["invoice_no", "shipment_ref", "charge_type", "amount"]),
    "invoices_swf": ("ref", ["inv_id", "ref", "description", "amount_inr"]),
    "invoices_trk": ("consignment", ["invoice_number", "consignment", "line_type", "net_amt"]),
}
for name, (ref_col, key) in INVOICES.items():
    inv = bronze(name)
    dups = inv.groupBy(*key).count().agg(F.sum(F.col("count") - 1)).first()[0] or 0
    orphans = (inv.withColumn("r", normalise_ref(F.col(ref_col)))
               .join(ship_refs.withColumnRenamed("ref", "r"), "r", "left_anti").count())
    report(name, "carrier_invoices", inv, [],
           extra=[("duplicate_lines", "info", int(dups)), ("orphan_no_shipment", "info", int(orphans))])

spark.stop()
