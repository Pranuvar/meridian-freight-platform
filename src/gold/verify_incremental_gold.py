"""Checks for incremental Gold: completeness, what the increment changed (time travel), late-arrival fixes,
SCD2 history stability, and a comparison against the generator's truth tables (initial load + the week)."""
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.gold.common import gold_path
from src.silver.common import silver_path
from src.utils.config import DATA
from src.utils.spark import get_spark
from src.utils.state import FMT, STATE

spark = get_spark("gold_incremental_verify")
gold = lambda n: spark.read.format("delta").load(gold_path(n))        # noqa: E731
silver = lambda n: spark.read.format("delta").load(silver_path(n))    # noqa: E731

print("\n--- 1. completeness: Gold rows = distinct keys = Silver rows ---")
for fact, key, src in [("fact_shipment", "shipment_key", "shipments"), ("fact_tracking_event", "event_key", "scans"),
                       ("fact_carrier_invoice_line", "invoice_line_key", "carrier_invoice_lines")]:
    f = gold(fact)
    n, k, sv = f.count(), f.select(key).distinct().count(), silver(src).count()
    print(f"{fact:<28} rows={n:>10,}  distinct keys={k:>10,}  silver rows={sv:>10,}  {'PASS' if n == k == sv else 'FAIL'}")

print("\n--- 2. what the increment changed in fact_shipment (Delta time travel: before the first MERGE vs now) ---")
path = gold_path("fact_shipment")
first = (DeltaTable.forPath(spark, path).history().filter("operation = 'MERGE'")
         .orderBy("version").select("version").first())
if first is None:
    print("no MERGE yet: run python -m src.gold.incremental first")
else:
    before, now = spark.read.format("delta").option("versionAsOf", first["version"] - 1).load(path), gold("fact_shipment")
    cols = ["customer_key", "carrier_key", "lane_key", "booked_date_key", "quoted_carrier_cost_inr",
            "revenue_total_inr", "status", "delivered_at", "is_delivered", "is_on_time", "transit_hours",
            "scan_count", "invoice_line_count", "carrier_cost_invoiced_inr", "margin_inr", "claim_count"]
    j = now.alias("a").join(before.alias("b"), F.col("a.shipment_key") == F.col("b.shipment_key"), "inner")
    row = j.agg(F.count("*").alias("_n"), *[
        F.sum(F.when(~F.col(f"a.{c}").eqNullSafe(F.col(f"b.{c}")), 1).otherwise(0)).alias(c) for c in cols]).first().asDict()
    print(f"rows before: {before.count():,}   rows now: {now.count():,}   shipments present in both: {row['_n']:,}")
    print("the first six must be 0 (history stays put); the rest show what late and new data fixed:")
    for i, c in enumerate(cols):
        print(f"  {c:<28} changed on {int(row[c] or 0):>7,} existing shipments" + ("   <- must be 0" if i < 6 else ""))

print("\n--- 3. late-arriving DELIVERED scans (event on/before 31 May, arrived in June) ---")
late = (silver("scans").filter("event_type = 'DELIVERED' AND arrival_date >= '2026-06-01' AND event_date <= '2026-05-31'")
        .select("shipment_ref").distinct())
lj = late.join(gold("fact_shipment"), "shipment_ref", "inner")
print(f"shipments with such a scan: {late.count():,} | in the fact: {lj.count():,} | "
      f"now is_delivered: {lj.filter('is_delivered').count():,} | delivered_date_key on/before 20260531: "
      f"{lj.filter('delivered_date_key <= 20260531').count():,}")

print("\n--- 4. SCD2: new versions from the 2026-06-03 reassignments, and where shipments landed ---")
dc = gold("dim_customer").filter("customer_id <> -1")
print("dim_customer rows:", gold("dim_customer").count(), "| versions starting 2026-06-03:",
      dc.filter("effective_from = '2026-06-03'").count())
ver = dc.filter("effective_from = '2026-06-03'").select("customer_id")
fs2 = gold("fact_shipment").join(dc.select("customer_key", "customer_id", "account_manager", "effective_from",
                                           "effective_to"), "customer_key")
top = fs2.join(ver, "customer_id").groupBy("customer_id").count().orderBy(F.desc("count")).first()["customer_id"]
(fs2.filter(F.col("customer_id") == top).groupBy("account_manager", "effective_from", "effective_to")
    .agg(F.count("*").alias("shipments"), F.min("booked_at").alias("first_booked"), F.max("booked_at").alias("last_booked"))
    .orderBy("effective_from").show(truncate=False))

print("--- 5. pipeline vs the generator's truth (initial load + the week) ---")
T = DATA / "truth"
t0, t1 = pd.read_parquet(T / "shipment_truth.parquet"), pd.read_parquet(T / "shipment_truth_inc1.parquet")
inv1 = pd.read_parquet(T / "invoice_truth_inc1.parquet").set_index("shipment_id")["carrier_invoiced_true_inc1"]
truth = pd.concat([t0, t1], ignore_index=True)
# Shipments un-invoiced at the cutoff were invoiced in the week: fill in their true carrier cost.
truth["carrier_invoiced_true"] = truth["carrier_invoiced_true"].fillna(truth["shipment_id"].map(inv1))
truth["truth_delivered_s"] = (truth["delivered_at"] - pd.Timestamp("1970-01-01")).dt.total_seconds()
truth["truth_carrier"] = truth["carrier_id"].map({1: "BLZ", 2: "SWF", 3: "TRK"})

fs = (gold("fact_shipment").join(gold("dim_carrier").select("carrier_key", "carrier_code"), "carrier_key")
      .select("shipment_ref", "carrier_code", "billing_currency", "is_on_time",
              F.col("delivered_at").cast("long").alias("delivered_s"),
              F.col("revenue_total_inr").cast("double"), F.col("carrier_cost_invoiced_inr").cast("double")).toPandas())
m = truth.merge(fs, on="shipment_ref", how="inner")
d = m[m["delivered_s"].notna()]
print(f"shipments with a DELIVERED scan in the fact: {len(d):,}")
print("delivered-timestamp mismatches :", int((d["delivered_s"] != d["truth_delivered_s"]).sum()))
print("on-time flag mismatches        :", int((d["is_on_time"].astype(bool) != d["on_time"].astype(bool)).sum()))
due = m[m["delivered_at"] <= pd.Timestamp("2026-06-07 23:59:59")]
print(f"truth: delivered by 7 June and in the fact: {len(due):,} | with a DELIVERED scan: {int(due['delivered_s'].notna().sum()):,}"
      f" (gap {len(due) - int(due['delivered_s'].notna().sum()):,}: dropped scans, blank types, corrupted refs)")
cmp = pd.DataFrame({"truth_%": due.groupby("truth_carrier")["on_time"].mean() * 100,
                    "pipeline_%": d.groupby("carrier_code")["is_on_time"].apply(lambda x: x.astype(bool).mean() * 100)})
cmp.loc["ALL"] = [due["on_time"].mean() * 100, d["is_on_time"].astype(bool).mean() * 100]
cmp["diff_pp"] = cmp["pipeline_%"] - cmp["truth_%"]
print("\non-time % (truth vs pipeline):\n" + cmp.round(2).to_string())

print("\nrevenue (INR), pipeline vs truth:")
for cur in ["INR", "USD"]:
    x = m[m["billing_currency"] == cur]
    p, t = x["revenue_total_inr"].sum(), x["billed_inr"].sum()
    print(f"  {cur} ({len(x):>7,} shipments): pipeline {p:>17,.2f}   truth {t:>17,.2f}   diff {p - t:>13,.2f} ({(p - t) / t * 100:+.3f}%)")
c = m[m["carrier_cost_invoiced_inr"].notna() & m["carrier_invoiced_true"].notna()]
print(f"\ncarrier cost: {len(c):,} shipments compared | mismatches > Rs 0.02: "
      f"{int(((c['carrier_cost_invoiced_inr'] - c['carrier_invoiced_true']).abs() > 0.02).sum())} | "
      f"invoiced in truth but missing in pipeline: {int((m['carrier_invoiced_true'].notna() & m['carrier_cost_invoiced_inr'].isna()).sum())} | "
      f"invoiced in pipeline but not in truth: {int((m['carrier_invoiced_true'].isna() & m['carrier_cost_invoiced_inr'].notna()).sum())}")

print("\n--- 6. Gold watermarks ---")
spark.read.format("delta").load(STATE).filter("key LIKE 'gold.%'").select(
    "key", F.date_format("ts", FMT).alias("watermark")).orderBy("key").show(truncate=False)
spark.stop()
