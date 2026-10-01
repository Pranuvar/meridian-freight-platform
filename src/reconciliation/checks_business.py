"""Reconciliation part 2: do the money and the relationships still hold in Gold?"""
import pandas as pd
from pyspark.sql import functions as F

from src.gold.common import gold_path
from src.reconciliation.checks_flow import json_field, quarantine, read_delta
from src.silver.common import silver_path
from src.utils.config import DATA

LEDGER_RATE = 84.0     # the flat USD->INR rate the finance ledger books export shipments at


def layer_counts(spark, rc):
    for silver, gold, label in [("shipments", "fact_shipment", "shipments"),
                                ("scans", "fact_tracking_event", "scan events"),
                                ("carrier_invoice_lines", "fact_carrier_invoice_line", "invoice lines")]:
        rc.equal(f"{label}: Silver rows = Gold rows", "Silver", "Gold",
                 read_delta(spark, silver_path(silver)).count(), read_delta(spark, gold_path(gold)).count(),
                 note="one fact row per Silver row (the grain)")


def revenue(spark, rc):
    charges = read_delta(spark, silver_path("shipment_charges"))
    ships = read_delta(spark, silver_path("shipments")).select("shipment_id", F.col("currency").alias("ship_currency"))
    mismatch = charges.join(ships, "shipment_id").filter(F.col("currency") != F.col("ship_currency")).count()
    rc.equal("charges whose currency differs from their shipment's", "Silver", "Silver", 0, mismatch,
             note="Gold converts a whole shipment at ONE rate, so this must be 0 for the checks below to be valid")
    native = {r["currency"]: r["s"] for r in charges.groupBy("currency").agg(F.sum("amount").alias("s")).collect()}
    gold = (read_delta(spark, gold_path("fact_shipment")).groupBy("billing_currency")
            .agg(F.sum("revenue_total_inr").alias("inr"),
                 F.sum(F.col("revenue_total_inr").cast("double") / F.col("fx_rate_applied").cast("double")).alias("back")))
    g = {r["billing_currency"]: (r["inr"], r["back"]) for r in gold.collect()}
    rc.equal("revenue INR: Silver charges vs Gold", "Silver", "Gold", native["INR"], g["INR"][0],
             note="INR rate is 1.0000, so the comparison is exact")
    rc.equal("revenue USD: Silver charges vs Gold converted back", "Silver", "Gold", native["USD"], g["USD"][1], tol=1,
             note="Gold INR divided by the applied rate; $1 tolerance covers rounding each line to 2 decimals")


def fx_variance(spark, rc):
    """The finance ledger (here: the generator's truth tables) books USD at a flat rate. Gold uses the daily rate.
    The difference must equal the rate effect, and nothing else."""
    T = DATA / "truth"
    truth = pd.concat([pd.read_parquet(T / f, columns=["shipment_ref", "billed_inr"])
                       for f in ("shipment_truth.parquet", "shipment_truth_inc1.parquet")], ignore_index=True)
    fs = (read_delta(spark, gold_path("fact_shipment"))
          .select("shipment_ref", "billing_currency", F.col("revenue_total_inr").cast("double").alias("rev"),
                  F.col("fx_rate_applied").cast("double").alias("rate")).toPandas())    # ~200k rows x 4 columns
    m = truth.merge(fs, on="shipment_ref", how="inner")
    inr, usd = m[m.billing_currency == "INR"], m[m.billing_currency == "USD"]
    rc.equal("revenue INR: Gold vs finance ledger", "Gold", "Ledger", round(inr.billed_inr.sum(), 2),
             round(inr.rev.sum(), 2), tol=0.01, note=f"{len(inr):,} shipments")
    diff = usd.rev.sum() - usd.billed_inr.sum()
    # ledger value = native x 84, Gold value = native x rate, so Gold - ledger = native x (rate - 84)
    # = ledger value x (rate / 84 - 1).
    effect = (usd.billed_inr * (usd.rate / LEDGER_RATE - 1)).sum()
    rc.explained("revenue USD: Gold minus ledger = FX rate effect", "Gold", "FX effect", round(diff, 2),
                 round(effect, 2), tol=10,
                 note=f"{len(usd):,} USD shipments; INR 10 tolerance covers per-line rounding")


def carrier_cost(spark, rc):
    inv = read_delta(spark, gold_path("fact_carrier_invoice_line"))
    fs = read_delta(spark, gold_path("fact_shipment"))
    rc.equal("carrier invoices: Silver amount = Gold amount", "Silver", "Gold",
             read_delta(spark, silver_path("carrier_invoice_lines")).agg(F.sum("amount")).first()[0],
             inv.agg(F.sum("amount_inr")).first()[0])
    matched = inv.join(fs.select("shipment_key"), "shipment_key", "left_semi")
    rc.equal("carrier cost: invoice lines vs fact_shipment", "Invoice fact", "fact_shipment",
             fs.agg(F.sum("carrier_cost_invoiced_inr")).first()[0], matched.agg(F.sum("amount_inr")).first()[0],
             note="lines whose shipment has a fact row must add up to the shipment-level carrier cost exactly")


def orphans(spark, rc):
    fs = read_delta(spark, gold_path("fact_shipment")).select("shipment_key")
    qs = quarantine(spark, "shipments")
    rejected = qs.select(json_field(qs["record"], "shipment_ref").alias("shipment_ref")).distinct()
    for fact, label in [("fact_tracking_event", "scan events"), ("fact_carrier_invoice_line", "invoice lines")]:
        orphan = read_delta(spark, gold_path(fact)).join(fs, "shipment_key", "left_anti")
        rc.explained(f"{label} with no fact_shipment row", fact, "quarantined shipments", orphan.count(),
                     orphan.join(rejected, "shipment_ref", "left_semi").count(),
                     note="their shipment was rejected in Silver; the events and lines were kept on purpose")


def delivered_gap(spark, rc):
    """TMS says DELIVERED, but no DELIVERED scan exists. Split the gap into what can be attributed EXACTLY,
    what can only be ESTIMATED, and a residual that is compared with the historical scanner-loss rate."""
    fs = read_delta(spark, gold_path("fact_shipment"))
    gap_df = fs.filter("status_delivered_no_scan")
    gap, delivered = gap_df.count(), fs.filter("status = 'DELIVERED'").count()
    rc.equal("DELIVERED shipments with no scan at all", "TMS status", "scans", 0,
             fs.filter("status = 'DELIVERED' AND scan_count = 0").count())

    qs = quarantine(spark, "scans")
    # EXACT: a DELIVERED scan whose shipment reference was corrupted is quarantined as an orphan.
    orphan_del = qs.filter(F.col("reject_reasons").contains("orphan_no_shipment")
                           & (json_field(qs["record"], "event_type") == "DELIVERED")).count()
    # ESTIMATE: a scan with a blank event type could have been any of the 10 event types; 1 in 10 is DELIVERED.
    blank_est = round(qs.filter(F.col("reject_reasons").contains("event_type_missing")).count() / 10)
    # EXACT (simulation only): DELIVERED scans still sitting in the held-back files, i.e. not arrived yet.
    # In production this is "data the source has dispatched but we have not received".
    held = pd.read_parquet(DATA / "held_back" / "scans_held_after_inc1.parquet", columns=["shipment_ref", "event_type"])
    held = held[held.event_type == "DELIVERED"]
    held_ref = held.shipment_ref.str.replace(r"^MFL-?(\d{4})-?(\d{7})$", r"MFL-\1-\2", regex=True)
    gap_refs = set(gap_df.select("shipment_ref").toPandas()["shipment_ref"])
    held_n = int(held_ref[held_ref.isin(gap_refs)].nunique())

    residual = gap - orphan_del - blank_est - held_n
    rc.info("delivered-without-scan: total gap", "TMS status", "scans", gap, note=f"of {delivered:,} DELIVERED shipments")
    rc.info("  attributed exactly: DELIVERED scan quarantined as orphan", "scans", "quarantine", orphan_del)
    rc.info("  attributed exactly: DELIVERED scan not arrived yet", "scans", "held-back files", held_n)
    rc.info("  estimated: blank event type (1 in 10 would be DELIVERED)", "scans", "quarantine", blank_est)
    rc.info("  residual: the source never sent it", "scans", "-", residual)
    rc.band("  residual as % of DELIVERED shipments", "residual", "-", round(residual / delivered * 100, 3), 0.5, 1.5,
            note="historical scanner-loss norm is about 1%")
