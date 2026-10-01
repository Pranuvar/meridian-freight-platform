"""Gold fact checks: grain, foreign keys, and a comparison against the generator's truth table."""
import pandas as pd
from pyspark.sql import functions as F

from src.gold.common import gold_path
from src.silver.common import silver_path
from src.utils.config import DATA
from src.utils.spark import get_spark

spark = get_spark("gold_facts_verify")


def gold(name):
    return spark.read.format("delta").load(gold_path(name))


def silver(name):
    return spark.read.format("delta").load(silver_path(name))


print("\n--- 1. grain and completeness (rows = distinct keys = Silver rows) ---")
for fact, key, src in [("fact_shipment", "shipment_key", "shipments"), ("fact_tracking_event", "event_key", "scans"),
                       ("fact_carrier_invoice_line", "invoice_line_key", "carrier_invoice_lines")]:
    f = gold(fact)
    n, k, s = f.count(), f.select(key).distinct().count(), silver(src).count()
    print(f"{fact:<28} rows={n:>9,}  distinct keys={k:>9,}  silver rows={s:>9,}  {'PASS' if n == k == s else 'FAIL'}")

print("\n--- 2. rows pointing at the Unknown member (-1) ---")
checks = {"fact_shipment": ["customer_key", "carrier_key", "origin_location_key", "dest_location_key",
                            "lane_key", "service_level_key"],
          "fact_tracking_event": ["location_key"], "fact_carrier_invoice_line": ["carrier_key"]}
for fact, cols in checks.items():
    f = gold(fact)
    row = f.agg(*[F.sum(F.when(F.col(c) == -1, 1).otherwise(0)).alias(c) for c in cols]).first().asDict()
    print(f"{fact:<28} of {f.count():>9,} rows:", {c: int(row[c] or 0) for c in cols})

print("\n--- 3. pipeline vs the generator's truth table ---")
fs = (gold("fact_shipment").join(gold("dim_carrier").select("carrier_key", "carrier_code"), "carrier_key")
      .select("shipment_ref", "carrier_code", "billing_currency", "is_on_time",
              F.col("status").alias("status_p"), F.col("delivered_at").cast("long").alias("delivered_s"),
              F.col("revenue_total_inr").cast("double"), F.col("carrier_cost_invoiced_inr").cast("double"))
      .toPandas())          # ~200k rows x 8 columns: small enough to bring to the driver

truth = pd.read_parquet(DATA / "truth" / "shipment_truth.parquet")
t0 = pd.Timestamp("1970-01-01")
truth["truth_delivered_s"] = (truth["delivered_at"] - t0).dt.total_seconds()    # epoch seconds: no timezone ambiguity
truth["truth_carrier"] = truth["carrier_id"].map({1: "BLZ", 2: "SWF", 3: "TRK"})

delivered = truth[truth["status"] == "DELIVERED"]
m = delivered.merge(fs, on="shipment_ref", how="left")
in_fact = int(m["status_p"].notna().sum())
with_scan = int(m["delivered_s"].notna().sum())
print(f"truth: shipments delivered by the cutoff : {len(m):,}")
print(f"  found in fact_shipment                 : {in_fact:,}  (gap {len(m) - in_fact:,}: rejected in Silver)")
print(f"  ...and with a DELIVERED scan           : {with_scan:,}  (gap {in_fact - with_scan:,}: scan missing, blank type, or arrived after cutoff)")

d = m[m["delivered_s"].notna()]
print("delivered-timestamp mismatches           :", int((d["delivered_s"] != d["truth_delivered_s"]).sum()), f"of {len(d):,}")
print("on-time flag mismatches                  :", int((d["is_on_time"].astype(bool) != d["on_time"].astype(bool)).sum()))

cmp = pd.DataFrame({
    "truth_%": delivered.groupby("truth_carrier")["on_time"].mean() * 100,
    "pipeline_%": d.groupby("carrier_code")["is_on_time"].apply(lambda s: s.astype(bool).mean() * 100)})
cmp.loc["ALL"] = [delivered["on_time"].mean() * 100, d["is_on_time"].astype(bool).mean() * 100]
cmp["diff_pp"] = cmp["pipeline_%"] - cmp["truth_%"]
print("\non-time % (truth vs pipeline):\n" + cmp.round(2).to_string())

allm = truth.merge(fs, on="shipment_ref", how="inner")
print("\nrevenue (INR) - pipeline vs truth:")
for cur in ["INR", "USD"]:
    x = allm[allm["billing_currency"] == cur]
    p, t = x["revenue_total_inr"].sum(), x["billed_inr"].sum()
    print(f"  {cur} shipments ({len(x):>7,}): pipeline {p:>17,.2f}   truth {t:>17,.2f}   diff {p - t:>13,.2f} ({(p - t) / t * 100:+.3f}%)")
print("  (USD differs on purpose: pipeline uses the daily FX rate, the generator used a flat 84.0)")

c = allm[allm["carrier_cost_invoiced_inr"].notna() & allm["carrier_invoiced_true"].notna()]
bad = int(((c["carrier_cost_invoiced_inr"] - c["carrier_invoiced_true"]).abs() > 0.02).sum())
lost = int((allm["carrier_invoiced_true"].notna() & allm["carrier_cost_invoiced_inr"].isna()).sum())
print(f"\ncarrier cost: {len(c):,} shipments compared; per-shipment mismatches > Rs 0.02: {bad}; "
      f"invoiced in truth but missing in pipeline: {lost}")
print("delivered-status shipments with no DELIVERED scan (flagged in the fact):",
      gold("fact_shipment").filter("status_delivered_no_scan").count())

print("\n--- 4. SCD2 payoff: one customer's shipments land on the version valid at booking ---")
dc = gold("dim_customer").filter("customer_id <> -1")
fs_ = gold("fact_shipment").join(
    dc.select("customer_key", "customer_id", "account_manager", "effective_from", "effective_to"), "customer_key")
three = dc.groupBy("customer_id").count().filter("count >= 3").select("customer_id")
top = fs_.join(three, "customer_id").groupBy("customer_id").count().orderBy(F.desc("count")).first()["customer_id"]
(fs_.filter(F.col("customer_id") == top)
    .groupBy("account_manager", "effective_from", "effective_to")
    .agg(F.count("*").alias("shipments"), F.min("booked_at").alias("first_booked"), F.max("booked_at").alias("last_booked"))
    .orderBy("effective_from").show(truncate=False))

print("--- 5. preview of the business story (delivered + invoiced shipments) ---")
(gold("fact_shipment").filter("carrier_cost_invoiced_inr IS NOT NULL")
    .join(gold("dim_carrier").select("carrier_key", "carrier_name"), "carrier_key")
    .groupBy("carrier_name")
    .agg(F.count("*").alias("shipments"),
         F.round(F.sum("margin_inr") / F.sum("revenue_total_inr") * 100, 2).alias("margin_pct"),
         F.round(F.avg(F.col("is_overbilled").cast("int")) * 100, 2).alias("overbilled_pct"))
    .orderBy("carrier_name").show(truncate=False))
spark.stop()
