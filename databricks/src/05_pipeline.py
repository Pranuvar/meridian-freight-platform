# Databricks notebook source
# MAGIC %run ./02_common

# COMMAND ----------

# MAGIC %run ./02b_common_more

# COMMAND ----------

# MAGIC %run ./04_gold_lib

# COMMAND ----------

print("--- latest reconciliation ---")
for r in spark.sql(f"SELECT status, count(*) AS n FROM {OPS}.recon_results WHERE run_ts = (SELECT max(run_ts) FROM {OPS}.recon_results) GROUP BY 1 ORDER BY 1").collect():
    print(f"  {r['status']:<10} {r['n']}")
print("--- published versions ---")
for r in spark.sql(f"SELECT published_at, run_id, table_name, delta_version FROM {OPS}.published_versions ORDER BY published_at DESC, table_name LIMIT 9").collect():
    print(f"  {r['published_at']}  run {r['run_id']}  {r['table_name']:<28} v{r['delta_version']}")

# COMMAND ----------

import os, time, zipfile

ROOT = f"/Volumes/{catalog}/meridian_landing/raw"
LANDING, CKPT = f"{ROOT}/landing", f"{ROOT}/_checkpoints"
CTL = f"{LANDING}/control"            # control files (source totals, ledger) unpacked once in Stage 4d

# Notebook parameters. A job task fills these through "Parameters"; run by hand they keep these defaults.
dbutils.widgets.text("step", "check_landing")    # which step this run performs: see STEPS below
dbutils.widgets.text("run_id", "manual")         # the job passes {{job.run_id}}
dbutils.widgets.text("fail_once_at", "")         # simulate a TRANSIENT failure in this step (a retry heals it)
dbutils.widgets.text("fail_at", "")              # simulate a PERMANENT failure in this step (needs a repair run)

# COMMAND ----------

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

CTL = f"{LANDING}/control"                    # files exported from the local pipeline (control_export.py)
totals = json.load(open(f"{CTL}/source_totals.json"))      # independent counts taken from Postgres at the source
LEDGER_RATE = 84.0     # the flat USD->INR rate the finance ledger books export shipments at
ROW_TOLERANCE = 50     # a handful of rows can legitimately fall between two accounting buckets (see ledger())

# ---------------------------------------------------------------- the framework
# Four outcomes:  PASS (two numbers that must agree do)  EXPLAINED (an EXPECTED gap, fully classified: nothing left over)
#                 INFO (a number worth seeing)           FAIL (something does not add up: a reconciliation BREAK)
@dataclass
class Result:
    check: str
    from_layer: str
    to_layer: str
    left: Optional[Decimal]
    right: Optional[Decimal]
    difference: Optional[Decimal]
    status: str
    note: str = ""

def _num(x):
    return None if x is None else Decimal(str(x))       # via str(): a float 0.1 becomes Decimal('0.1'), not its binary expansion

class Recon:
    def __init__(self):
        self.results = []
    def equal(self, check, f, t, expected, actual, tol=0, note=""):
        """Two numbers that must agree. tol is an ABSOLUTE tolerance (0 = exact)."""
        e, a = _num(expected), _num(actual)
        d = a - e
        ok = abs(d) <= Decimal(str(tol))
        self.results.append(Result(check, f, t, e, a, d, "PASS" if ok else "FAIL", note))
    def explained(self, check, f, t, gap, explained, tol=0, note=""):
        """A gap that is allowed to exist but must be completely classified. left = what is UNEXPLAINED."""
        g, x = _num(gap), _num(explained)
        left = g - x
        ok = abs(left) <= Decimal(str(tol))
        self.results.append(Result(check, f, t, g, x, left, "EXPLAINED" if ok else "FAIL", note))
    def band(self, check, f, t, value, lo, hi, note=""):
        """Statistical reasonableness: the value must sit inside a historical range."""
        v = _num(value)
        ok = Decimal(str(lo)) <= v <= Decimal(str(hi))
        self.results.append(Result(check, f, t, v, None, None, "PASS" if ok else "FAIL", f"expected between {lo} and {hi}. {note}"))
    def info(self, check, f, t, value, note=""):
        self.results.append(Result(check, f, t, _num(value), None, None, "INFO", note))
    @property
    def failed(self):
        return any(r.status == "FAIL" for r in self.results)
    def report(self):
        def fmt(v):
            if v is None:
                return ""
            return f"{int(v):,}" if v == v.to_integral_value() else f"{v.quantize(Decimal('0.01')):,}"
        w = max(len(r.check) for r in self.results)
        print(f"\n{'check':<{w}}  {'flow':<36}{'left':>20}{'right':>20}{'difference':>16}  status")
        print("-" * (w + 100))
        for r in self.results:
            print(f"{r.check:<{w}}  {r.from_layer + ' -> ' + r.to_layer:<36}{fmt(r.left):>20}{fmt(r.right):>20}{fmt(r.difference):>16}  {r.status}")
            if r.note:
                print(f"{'':<{w}}    {r.note}")
        counts = {s: sum(1 for r in self.results if r.status == s) for s in ("PASS", "EXPLAINED", "INFO", "FAIL")}
        print("\n" + "   ".join(f"{s}: {c}" for s, c in counts.items()))
        print("RECONCILIATION: " + ("BREAKS FOUND" if self.failed else "CLEAN (every gap is zero or fully explained)"))
    def save(self):
        """Append this run to a control table so the history is queryable (and chartable on a dashboard)."""
        ts = datetime.datetime.now(datetime.timezone.utc)
        s = lambda v: None if v is None else str(v)
        rows = [(ts, r.check, r.from_layer, r.to_layer, s(r.left), s(r.right), s(r.difference), r.status, r.note) for r in self.results]
        schema = ("run_ts timestamp, check_name string, from_layer string, to_layer string, left_value string, "
                  "right_value string, difference string, status string, note string")
        spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(f"{OPS}.recon_results")

# ---------------------------------------------------------------- helpers
def quarantine_of(table):
    return spark.table(QUARANTINE).filter(F.col("table_name") == table)

def json_field(col, name):
    """Quarantine keeps each rejected record as JSON text; this reads one field back out of it."""
    return F.get_json_object(col, f"$.{name}")

def reasons(qt, key):
    """Reject reasons by DISTINCT key (a row rejected in two batches counts once)."""
    rows = (qt.select(json_field(qt["record"], key).alias("k"), "reject_reasons").distinct().groupBy("reject_reasons").count().collect())
    return "; ".join(f"{r['reject_reasons']} = {r['count']:,}" for r in sorted(rows, key=lambda r: -r["count"])) or "none"

def reason_rows(qt):
    rows = qt.groupBy("reject_reasons").count().collect()
    return "; ".join(f"{r['reject_reasons']} = {r['count']:,}" for r in sorted(rows, key=lambda r: -r["count"])) or "none"

# ---------------------------------------------------------------- 1. does every record and rupee survive source -> Bronze -> Silver (+ quarantine)?
ENTITIES = [("customers", "customer_id"), ("shipments", "shipment_id"), ("shipment_charges", "charge_id"), ("claims", "claim_id")]

def entity_flow(rc):
    for table, key in ENTITIES:
        source = totals["entities"][table]
        b_keys = spark.table(f"{BRONZE}.tms_{table}").select(F.col(key).cast("long").alias(key)).distinct()
        s_keys = silver(table).select(F.col(key).cast("long").alias(key)).distinct()
        qt = quarantine_of(table)
        q_keys = qt.select(json_field(qt["record"], key).cast("long").alias(key)).distinct()
        n_b, n_s = b_keys.count(), s_keys.count()
        only_q = q_keys.join(s_keys, key, "left_anti").count()                         # rejected and NOT in Silver
        both = s_keys.join(q_keys, key, "inner").count()
        neither = b_keys.join(s_keys.unionByName(q_keys), key, "left_anti").count()    # lost without trace
        rc.equal(f"{table}: record count", "Postgres", "Bronze", source, n_b,
                 note="distinct keys: Bronze keeps every version of a row, and the crash test wrote some twice")
        rc.explained(f"{table}: rows missing from Silver", "Postgres", "quarantine", source - n_s, only_q, note=reasons(qt, key))
        rc.equal(f"{table}: keys in neither Silver nor quarantine", "Bronze", "Silver+quarantine", 0, neither)
        rc.info(f"{table}: keys in BOTH Silver and quarantine", "Silver", "quarantine", both,
                note="a row rejected once that later arrived valid; expected 0")

def charge_amounts(rc):
    def by_currency(df):
        rows = df.groupBy("currency").agg(F.count("*").alias("n"), F.sum("amount").alias("s")).collect()
        return {r["currency"]: (r["n"], r["s"]) for r in rows}
    src = {c: (v["n"], Decimal(v["s"])) for c, v in totals["charges_by_currency"].items()}
    silver_c = by_currency(silver("shipment_charges"))
    qt = quarantine_of("shipment_charges")
    quarantined = by_currency(qt.select(json_field(qt["record"], "charge_id").cast("long").alias("id"),
                                        json_field(qt["record"], "currency").alias("currency"),
                                        json_field(qt["record"], "amount").cast("decimal(14,2)").alias("amount")).distinct())
    zero = (0, Decimal("0.00"))
    for cur in sorted(src):
        (sn, ss), (qn, qs) = silver_c.get(cur, zero), quarantined.get(cur, zero)
        rc.equal(f"charges {cur}: row count", "Postgres", "Silver+quarantine", src[cur][0], sn + qn)
        rc.equal(f"charges {cur}: amount", "Postgres", "Silver+quarantine", src[cur][1], ss + qs, note=f"native {cur}, exact to the smallest unit")

def ledger(rc, name, n_bronze, n_silver, n_quar, n_groups, lo, hi, note):
    """Bronze rows = Silver + quarantine + duplicates. Duplicates are RECOMPUTED from Bronze (rows minus distinct business keys),
    not read from the pipeline's own log, so the check is independent of the code it verifies. ROW_TOLERANCE: a duplicate pair
    split across the initial load and an increment whose surviving row is rejected is counted as two rejects, not one reject
    plus one duplicate. That can move a few rows between buckets, never change the total."""
    dup_independent = n_bronze - n_groups
    rc.equal(f"{name}: Bronze = Silver + quarantine + duplicates", "Bronze", "Silver+quarantine+dups", n_bronze,
             n_silver + n_quar + dup_independent, tol=ROW_TOLERANCE,
             note=f"{n_bronze:,} = {n_silver:,} + {n_quar:,} + {dup_independent:,} (pipeline's own residual: {n_bronze - n_silver - n_quar:,})")
    rc.band(f"{name}: duplicate rate %", "Bronze", "-", round(dup_independent / n_bronze * 100, 3), lo, hi, note=note)

def scans_flow(rc):
    bronze = spark.table(f"{BRONZE}.scans")
    prepared = (bronze.withColumn("_ref_std", normalise_ref(F.col("shipment_ref"))).withColumn("_ts", parse_event_ts())
                      .withColumn("_loc_std", F.upper(F.trim("location_code"))))
    groups = prepared.groupBy("_ref_std", "event_type", "_ts", "_loc_std").count().count()    # groupBy treats NULL as a value, like the Silver window
    qt = quarantine_of("scans")
    ledger(rc, "scans", bronze.count(), silver("scans").count(), qt.count(), groups, 1.5, 2.5, "scanners re-send about 2% of scans")
    rc.info("scans: quarantine reasons", "Silver", "quarantine", qt.count(), note=reason_rows(qt))

def invoices_flow(rc):
    b = lambda n: spark.table(f"{BRONZE}.{n}")
    parsed = parse_blz(b("invoices_blz")).unionByName(parse_swf(b("invoices_swf"))).unionByName(parse_trk(b("invoices_trk")))
    prepared = parsed.withColumn("_ref_std", normalise_ref(F.col("_ref_raw")))
    groups = prepared.groupBy("carrier_code", "invoice_no", "_ref_std", "charge_type").count().count()
    qt = quarantine_of("carrier_invoice_lines")
    ledger(rc, "invoice lines", parsed.count(), silver("carrier_invoice_lines").count(), qt.count(), groups, 2.5, 3.5,
           "carriers re-send about 3% of lines")
    rc.info("invoice lines: quarantine reasons", "Silver", "quarantine", qt.count(), note=reason_rows(qt))

# ---------------------------------------------------------------- 2. do the money and the relationships still hold in Gold?
def layer_counts(rc):
    for s_name, g_name, label in [("shipments", "fact_shipment", "shipments"), ("scans", "fact_tracking_event", "scan events"),
                                  ("carrier_invoice_lines", "fact_carrier_invoice_line", "invoice lines")]:
        rc.equal(f"{label}: Silver rows = Gold rows", "Silver", "Gold", silver(s_name).count(), gold(g_name).count(),
                 note="one fact row per Silver row (the grain)")

def revenue(rc):
    charges = silver("shipment_charges")
    ships = silver("shipments").select("shipment_id", F.col("currency").alias("ship_currency"))
    mismatch = charges.join(ships, "shipment_id").filter(F.col("currency") != F.col("ship_currency")).count()
    rc.equal("charges whose currency differs from their shipment's", "Silver", "Silver", 0, mismatch,
             note="Gold converts a whole shipment at ONE rate, so this must be 0 for the checks below to be valid")
    native = {r["currency"]: r["s"] for r in charges.groupBy("currency").agg(F.sum("amount").alias("s")).collect()}
    g = (gold("fact_shipment").groupBy("billing_currency")
         .agg(F.sum("revenue_total_inr").alias("inr"),
              F.sum(F.col("revenue_total_inr").cast("double") / F.col("fx_rate_applied").cast("double")).alias("back")).collect())
    g = {r["billing_currency"]: (r["inr"], r["back"]) for r in g}
    rc.equal("revenue INR: Silver charges vs Gold", "Silver", "Gold", native["INR"], g["INR"][0], note="INR rate is 1.0000, so the comparison is exact")
    rc.equal("revenue USD: Silver charges vs Gold converted back", "Silver", "Gold", native["USD"], g["USD"][1], tol=1,
             note="Gold INR divided by the applied rate; $1 tolerance covers rounding each line to 2 decimals")

def fx_variance(rc):
    """The finance ledger (here: the generator's answer key) books USD at a flat rate. Gold uses the daily rate. The difference
    must equal the rate effect, and nothing else."""
    led = spark.read.parquet(f"{CTL}/ledger_billed.parquet").select("shipment_ref", F.col("billed_inr").cast("double").alias("billed_inr"))
    fs = gold("fact_shipment").select("shipment_ref", "billing_currency", F.col("revenue_total_inr").cast("double").alias("rev"),
                                      F.col("fx_rate_applied").cast("double").alias("rate"))
    m = led.join(fs, "shipment_ref", "inner")
    inr = m.filter("billing_currency = 'INR'").agg(F.count("*").alias("n"), F.sum("billed_inr").alias("led"), F.sum("rev").alias("gold")).first()
    usd = (m.filter("billing_currency = 'USD'")
            .agg(F.count("*").alias("n"), F.sum("billed_inr").alias("led"), F.sum("rev").alias("gold"),
                 # ledger value = native x 84, Gold value = native x rate, so Gold - ledger = ledger value x (rate / 84 - 1)
                 F.sum(F.col("billed_inr") * (F.col("rate") / LEDGER_RATE - 1)).alias("effect")).first())
    rc.equal("revenue INR: Gold vs finance ledger", "Gold", "Ledger", round(inr["led"], 2), round(inr["gold"], 2), tol=0.01, note=f"{inr['n']:,} shipments")
    rc.explained("revenue USD: Gold minus ledger = FX rate effect", "Gold", "FX effect", round(usd["gold"] - usd["led"], 2),
                 round(usd["effect"], 2), tol=10, note=f"{usd['n']:,} USD shipments; INR 10 tolerance covers per-line rounding")

def carrier_cost(rc):
    inv, fs = gold("fact_carrier_invoice_line"), gold("fact_shipment")
    rc.equal("carrier invoices: Silver amount = Gold amount", "Silver", "Gold",
             silver("carrier_invoice_lines").agg(F.sum("amount")).first()[0], inv.agg(F.sum("amount_inr")).first()[0])
    matched = inv.join(fs.select("shipment_key"), "shipment_key", "left_semi")
    rc.equal("carrier cost: invoice lines vs fact_shipment", "Invoice fact", "fact_shipment",
             fs.agg(F.sum("carrier_cost_invoiced_inr")).first()[0], matched.agg(F.sum("amount_inr")).first()[0],
             note="lines whose shipment has a fact row must add up to the shipment-level carrier cost exactly")

def orphans(rc):
    fs = gold("fact_shipment").select("shipment_key")
    qs = quarantine_of("shipments")
    rejected = qs.select(json_field(qs["record"], "shipment_ref").alias("shipment_ref")).distinct()
    for fact, label in [("fact_tracking_event", "scan events"), ("fact_carrier_invoice_line", "invoice lines")]:
        orphan = gold(fact).join(fs, "shipment_key", "left_anti")
        rc.explained(f"{label} with no fact_shipment row", fact, "quarantined shipments", orphan.count(),
                     orphan.join(rejected, "shipment_ref", "left_semi").count(),
                     note="their shipment was rejected in Silver; the events and lines were kept on purpose")

def delivered_gap(rc):
    """TMS says DELIVERED, but no DELIVERED scan exists. Split the gap into what can be attributed EXACTLY, what can only be
    ESTIMATED, and a residual that is compared with the historical scanner-loss rate."""
    fs = gold("fact_shipment")
    gap_df = fs.filter("status_delivered_no_scan")
    gap, delivered = gap_df.count(), fs.filter("status = 'DELIVERED'").count()
    rc.equal("DELIVERED shipments with no scan at all", "TMS status", "scans", 0, fs.filter("status = 'DELIVERED' AND scan_count = 0").count())
    qs = quarantine_of("scans")
    # EXACT: a DELIVERED scan whose shipment reference was corrupted is quarantined as an orphan.
    orphan_del = qs.filter(F.col("reject_reasons").contains("orphan_no_shipment") & (json_field(qs["record"], "event_type") == "DELIVERED")).count()
    # ESTIMATE: a scan with a blank event type could have been any of the 10 event types; 1 in 10 is DELIVERED.
    blank_est = round(qs.filter(F.col("reject_reasons").contains("event_type_missing")).count() / 10)
    # EXACT (simulation only): DELIVERED scans still in the held-back files, i.e. dispatched by the source but not received yet.
    held_refs = spark.read.parquet(f"{CTL}/held_delivered_refs.parquet").select("shipment_ref").distinct()
    held_n = gap_df.select("shipment_ref").join(held_refs, "shipment_ref", "left_semi").count()
    residual = gap - orphan_del - blank_est - held_n
    rc.info("delivered-without-scan: total gap", "TMS status", "scans", gap, note=f"of {delivered:,} DELIVERED shipments")
    rc.info("  attributed exactly: DELIVERED scan quarantined as orphan", "scans", "quarantine", orphan_del)
    rc.info("  attributed exactly: DELIVERED scan not arrived yet", "scans", "held-back files", held_n)
    rc.info("  estimated: blank event type (1 in 10 would be DELIVERED)", "scans", "quarantine", blank_est)
    rc.info("  residual: the source never sent it", "scans", "-", residual)
    rc.band("  residual as % of DELIVERED shipments", "residual", "-", round(residual / delivered * 100, 3), 0.5, 1.5,
            note="historical scanner-loss norm is about 1%")

def reconcile():
    rc = Recon()
    entity_flow(rc); charge_amounts(rc); scans_flow(rc); invoices_flow(rc)
    layer_counts(rc); revenue(rc); fx_variance(rc); carrier_cost(rc); orphans(rc); delivered_gap(rc)
    rc.report()
    rc.save()
    return rc

# ---------------------------------------------------------------- the PUBLISH GATE
PUBLISHED, PUB = f"{OPS}.published_versions", f"`{catalog}`.meridian_pub"

def publish_gold(run_id="manual"):
    """Record WHICH VERSION of every Gold table consumers may read, and expose those exact versions as views in meridian_pub.
    Dashboards read the views, so a half-finished or failed run can never leak into a report."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PUB}")
    now, rows = datetime.datetime.now(datetime.timezone.utc), []
    for t in spark.sql(f"SHOW TABLES IN {GOLD}").collect():
        name = t["tableName"]
        v = int(spark.sql(f"DESCRIBE HISTORY {GOLD}.{name}").agg(F.max("version")).first()[0])
        rows.append((now, run_id, name, v))
        try:
            spark.sql(f"CREATE OR REPLACE VIEW {PUB}.{name} AS SELECT * FROM {GOLD}.{name} VERSION AS OF {v}")
        except Exception as e:                       # fall back to the version table alone if this workspace refuses time travel in views
            print(f"  view for {name} not created ({str(e)[:90]}); version still recorded")
    spark.createDataFrame(rows, "published_at timestamp, run_id string, table_name string, delta_version long").write.mode("append").saveAsTable(PUBLISHED)
    print(f"published {len(rows)} Gold tables at {now:%Y-%m-%d %H:%M:%S}")
    for r in rows:
        print(f"  {r[2]:<28} version {r[3]}")

def reconcile_and_publish(run_id="manual"):
    """The gate: publish ONLY if reconciliation found no FAIL. This is the one call the Stage 5 job will make."""
    rc = reconcile()
    if rc.failed:
        raise RuntimeError("reconciliation found breaks: NOT publishing")
    publish_gold(run_id)

# COMMAND ----------

FACTS = {"fact_shipment": "shipment_key", "fact_tracking_event": "event_key", "fact_carrier_invoice_line": "invoice_line_key"}

# ---- what changed? ----
def since(df, wm):
    """Silver rows written or updated AFTER the watermark. _silver_processed_at is stamped by the Silver jobs only on rows
    they inserted or updated, so it doubles as the change marker Gold needs."""
    return df.filter(F.col("_silver_processed_at") > F.lit(wm).cast("timestamp"))

def find_affected(ships, charges, claims, scans, invoices, wm):
    """Shipments whose fact row must be recomputed. Returns (shipment_id, shipment_ref). Charges and claims are keyed by
    shipment_id, scans and invoices by shipment_ref, so each side is translated through Silver shipments. The inner join
    also drops refs Silver has no shipment for (orphans)."""
    dictionary = ships.select("shipment_id", "shipment_ref")
    by_ref = (since(ships, wm).select("shipment_ref").unionByName(since(scans, wm).select("shipment_ref"))
              .unionByName(since(invoices, wm).select("shipment_ref")).distinct().join(dictionary, "shipment_ref", "inner"))
    by_id = (since(charges, wm).select("shipment_id").unionByName(since(claims, wm).select("shipment_id"))
             .distinct().join(dictionary, "shipment_id", "inner"))
    return by_ref.select("shipment_id", "shipment_ref").unionByName(by_id.select("shipment_id", "shipment_ref")).distinct()

def consumed_watermark(old, *frames):
    """Highest _silver_processed_at among the rows this run consumed. Never moves backwards."""
    vals = [v for v in (max_ts(since(f, old), "_silver_processed_at") for f in frames) if v]
    return max(vals) if vals else old

def gold_start_wm(name):
    """Saved watermark, else the time of the EARLIEST full-build row. MIN, not MAX: if a first run crashed after its merge
    but before saving its watermark, MAX would point past data it never finished and skip it forever."""
    key = f"gold.{name}"
    wm = get_ts(key)
    if wm.startswith("1900"):                    # never saved
        wm = gold(name).agg(F.date_format(F.min("_gold_processed_at"), FMT)).first()[0]
        print(f"  [{key}] no watermark yet: starting from the first full build, {wm}")
    return wm

# ---- merging ----
def gold_merge(name, key, df, update_condition=None, insert_only=False):
    """Matched -> update (optionally only when something really changed). Not matched -> insert. Returns (inserted, updated)."""
    df.createOrReplaceTempView("_gold_src")
    matched = "" if insert_only else f"WHEN MATCHED{(' AND ' + update_condition) if update_condition else ''} THEN UPDATE SET *\n"
    r = spark.sql(f"MERGE INTO {GOLD}.{name} t USING _gold_src s ON t.{key} = s.{key}\n{matched}WHEN NOT MATCHED THEN INSERT *").first()
    return int(r["num_inserted_rows"]), int(r["num_updated_rows"])

def check_grain(name, key, expected):
    t = gold(name)
    n, k = t.count(), t.select(key).distinct().count()
    ok = n == k == expected
    print(f"  {name:<28} rows={n:>10,}  distinct keys={k:>10,}  silver rows={expected:>10,}  {'PASS' if ok else 'FAIL'}")
    return ok

def gold_incremental():
    batch = new_batch("ginc")
    s = {n: silver(n) for n in ["shipments", "shipment_charges", "claims", "scans", "carrier_invoice_lines"]}
    print(f"gold incremental run {batch}")
    ok = True

    # ============================================================ fact_shipment
    wm = gold_start_wm("fact_shipment")
    affected = find_affected(s["shipments"], s["shipment_charges"], s["claims"], s["scans"], s["carrier_invoice_lines"], wm)
    new_wm = consumed_watermark(wm, *s.values())
    affected = stage(affected, "gold_affected")        # serverless has no persist(): materialise once, reuse
    n_aff, total = affected.count(), s["shipments"].count()
    if n_aff == 0:
        print("  fact_shipment                nothing to do")
    else:
        ins, upd = gold_merge("fact_shipment", "shipment_key", build_fact_shipment(batch, refs=affected))
        print(f"  fact_shipment                affected shipments={n_aff:>8,} ({n_aff / total:.1%} of {total:,})  -> inserted {ins:,}, updated {upd:,}")
        log_gold(batch, "fact_shipment", n_aff, ins + upd)
        ok &= (ins + upd == n_aff)                     # the rebuilt set must land 1:1, or the grain is broken
    ok &= check_grain("fact_shipment", "shipment_key", total)
    if ok and new_wm:
        set_ts("gold.fact_shipment", new_wm)

    # ============================================================ fact_tracking_event
    wm = gold_start_wm("fact_tracking_event")
    new_scans = since(s["scans"], wm)
    new_wm = max_ts(new_scans, "_silver_processed_at")
    refs = stage(new_scans.select("shipment_ref").distinct(), "gold_refs")
    n_refs = refs.count()
    if n_refs == 0:
        print("  fact_tracking_event          nothing to do")
    else:
        # ALL scans of these shipments are rebuilt (the window needs the complete timeline). Silver scans never change, so the
        # only value that can differ on an existing event is the gap to its predecessor: update only when that changed.
        ins, upd = gold_merge("fact_tracking_event", "event_key", build_fact_tracking(batch, refs=refs),
                              update_condition="NOT (t.hours_since_prev_scan <=> s.hours_since_prev_scan)")
        print(f"  fact_tracking_event          shipments with new scans={n_refs:>8,}  -> inserted {ins:,} events, re-gapped {upd:,} existing events")
        log_gold(batch, "fact_tracking_event", n_refs, ins + upd)
    ok &= check_grain("fact_tracking_event", "event_key", s["scans"].count())
    if ok and new_wm:
        set_ts("gold.fact_tracking_event", new_wm)

    # ============================================================ fact_carrier_invoice_line (insert-only)
    wm = gold_start_wm("fact_carrier_invoice_line")
    new_lines = since(s["carrier_invoice_lines"], wm)
    new_wm, n_new = max_ts(new_lines, "_silver_processed_at"), new_lines.count()
    if n_new == 0:
        print("  fact_carrier_invoice_line    nothing to do")
    else:
        ins, _ = gold_merge("fact_carrier_invoice_line", "invoice_line_key", build_fact_invoice(batch, since=wm), insert_only=True)
        print(f"  fact_carrier_invoice_line    new Silver lines={n_new:>8,}  -> inserted {ins:,}")
        log_gold(batch, "fact_carrier_invoice_line", n_new, ins)
        ok &= (ins == n_new)
    ok &= check_grain("fact_carrier_invoice_line", "invoice_line_key", s["carrier_invoice_lines"].count())
    if ok and new_wm:
        set_ts("gold.fact_carrier_invoice_line", new_wm)
    if not ok:
        raise RuntimeError("one or more Gold checks FAILED. Watermarks were not advanced for any fact that failed.")

def gold_backfill(start, end):
    """A CORRECTION, not new data: recompute the shipments BOOKED in [start, end]. Watermarks are not touched."""
    batch = new_batch("gback")
    ships = silver("shipments")
    window = ships.filter(F.to_date("booked_at").between(F.lit(start).cast("date"), F.lit(end).cast("date")))
    affected = stage(window.select("shipment_id", "shipment_ref"), "gold_backfill")
    n_aff = affected.count()
    ins, upd = gold_merge("fact_shipment", "shipment_key", build_fact_shipment(batch, refs=affected))
    print(f"gold backfill {start}..{end}: shipments={n_aff:,} -> inserted {ins:,}, updated {upd:,}")
    if not (ins + upd == n_aff and check_grain("fact_shipment", "shipment_key", ships.count())):
        raise RuntimeError("backfill failed its grain check")

def fp():
    """Content fingerprint per fact, returned as a dict so two states can be compared."""
    out = {}
    for name in FACTS:
        t = gold(name)
        cols = [c for c in t.columns if not c.startswith("_gold")]
        row = t.agg(F.count("*").alias("n"), F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("fp")).first()
        out[name] = (row["n"], int(row["fp"]))
    return out

# COMMAND ----------

from datetime import date

UNKNOWN_KEY = -1            # every dimension (except dim_date) has a row with this key: facts never lose a row to a missing join
FAR_PAST, FAR_FUTURE = "1900-01-01", "9999-12-31"     # start of the FIRST version / end of the CURRENT version of an SCD2 row

def sk(*cols):
    """Deterministic 64-bit surrogate key from natural-key columns. The same natural key always gives the same
    surrogate, so rebuilds and reruns never change keys that facts already use."""
    return F.xxhash64(*cols)

def log_gold(batch, table, n_in, n_out):
    row = [(datetime.datetime.now(datetime.timezone.utc), batch, "gold", table, int(n_in), int(n_out), 0, 0)]
    spark.createDataFrame(row, RUN_LOG_SCHEMA).write.mode("append").saveAsTable(RUN_LOG)

# Business-owned reference data (not in any source system): code, name, base transit days, sort order.
SERVICE_LEVELS = [("EXPRESS", "Express", 2, 1), ("STANDARD", "Standard", 4, 2), ("ECONOMY", "Economy", 7, 3)]

def build_dim_date(start="2024-01-01", end="2027-12-31"):
    days = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
    d, m, y = F.col("date"), F.month("date"), F.year("date")
    iso_dow = ((F.dayofweek(d) + 5) % 7) + 1          # Spark: 1=Sunday..7=Saturday  ->  ISO: 1=Monday..7=Sunday
    fy_start = F.when(m >= 4, y).otherwise(y - 1)     # India's financial year runs April to March
    return days.select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"), d.alias("date"), y.alias("year"),
        F.quarter(d).alias("quarter"), m.alias("month"), F.date_format(d, "MMMM").alias("month_name"),
        F.date_format(d, "yyyy-MM").alias("year_month"), F.trunc(d, "month").alias("month_start_date"),
        F.weekofyear(d).alias("iso_week"), iso_dow.alias("day_of_week"), F.date_format(d, "EEEE").alias("day_name"),
        (iso_dow >= 6).alias("is_weekend"),
        F.concat(F.lit("FY"), fy_start.cast("string"), F.lit("-"),
                 F.lpad(((fy_start + 1) % 100).cast("string"), 2, "0")).alias("fiscal_year"),
        (F.floor(((m - 4 + 12) % 12) / 3) + 1).cast("int").alias("fiscal_quarter"))

def with_unknown(dim, unknown_row):
    return dim.unionByName(spark.createDataFrame([unknown_row], schema=dim.schema))

def build_dim_location():
    src = spark.table(f"{SILVER}.locations")
    dim = src.select(sk(F.col("location_code")).alias("location_key"), "location_code", F.col("name").alias("location_name"),
                     "city", "state", "region", "location_type")
    return with_unknown(dim, (UNKNOWN_KEY, "UNKNOWN", "Unknown location", "Unknown", "Unknown", "Unknown", "UNKNOWN"))

def build_dim_carrier():
    src = spark.table(f"{SILVER}.carriers")
    dim = src.select(sk(F.col("carrier_code")).alias("carrier_key"), "carrier_code", "carrier_name",
                     F.col("mode").alias("transport_mode"), "is_active")
    return with_unknown(dim, (UNKNOWN_KEY, "UNKNOWN", "Unknown carrier", "UNKNOWN", False))

def build_dim_service_level():
    base = spark.createDataFrame(SERVICE_LEVELS, "service_level_code string, service_level_name string, base_transit_days int, sort_order int")
    dim = base.select(sk(F.col("service_level_code")).alias("service_level_key"), "*")
    return with_unknown(dim, (UNKNOWN_KEY, "UNKNOWN", "Unknown", None, 99))

def build_dim_lane(dim_location):
    """Every possible lane = every hub (origin) x every location (destination), minus same-place pairs.
    Built from reference data, not from shipments, so a lane exists before its first shipment."""
    loc = dim_location.filter(F.col("location_key") != UNKNOWN_KEY)
    origins = loc.filter("location_type = 'HUB'").select(F.col("location_key").alias("origin_location_key"),
                        F.col("location_code").alias("origin_code"), F.col("region").alias("origin_region"))
    dests = loc.select(F.col("location_key").alias("dest_location_key"), F.col("location_code").alias("dest_code"),
                       F.col("region").alias("dest_region"))
    lanes = origins.crossJoin(dests).filter("origin_code <> dest_code")
    dim = lanes.select(sk(F.col("origin_code"), F.col("dest_code")).alias("lane_key"),
                       F.concat_ws(" -> ", "origin_code", "dest_code").alias("lane_name"),
                       "origin_location_key", "origin_code", "origin_region", "dest_location_key", "dest_code", "dest_region",
                       (F.col("origin_region") != F.col("dest_region")).alias("is_inter_region"))
    return with_unknown(dim, (UNKNOWN_KEY, "Unknown lane", UNKNOWN_KEY, "UNKNOWN", "Unknown", UNKNOWN_KEY, "UNKNOWN", "Unknown", False))

def write_dim(batch, name, key, df):
    target = f"{GOLD}.{name}"
    df.coalesce(1).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)   # tiny table: one file
    written = spark.table(target)
    n_out, n_keys = written.count(), written.select(key).distinct().count()
    if n_keys != n_out:      # a duplicate natural key or a hash collision: stop, never publish it
        raise RuntimeError(f"{name}: {n_out - n_keys} duplicate surrogate keys")
    log_gold(batch, name, n_out, n_out)
    print(f"gold {name:<20} rows={n_out:>6,}  keys unique: PASS")

def build_static_dims():
    batch = new_batch("gold")
    location = build_dim_location()
    write_dim(batch, "dim_date", "date_key", build_dim_date())
    write_dim(batch, "dim_location", "location_key", location)
    write_dim(batch, "dim_carrier", "carrier_key", build_dim_carrier())
    write_dim(batch, "dim_service_level", "service_level_key", build_dim_service_level())
    write_dim(batch, "dim_lane", "lane_key", build_dim_lane(location))

# ---------------------------------------------------------------- dim_customer: SCD Type 2
DIM_CUSTOMER = f"{GOLD}.dim_customer"
TRACKED = ["segment", "account_manager", "region"]                 # Type 2: a change creates a NEW version
TYPE1 = ["customer_name", "payment_terms_days", "status"]          # Type 1: overwritten on ALL versions
DIM_COLS = ["customer_key", "customer_id", "customer_code", "customer_name", "segment", "account_manager", "region",
            "payment_terms_days", "status", "effective_from", "effective_to", "is_current", "row_hash"]

def customer_source():
    return spark.table(f"{SILVER}.customers").select("customer_id", "customer_code", "customer_name", "segment",
                       "account_manager", "region", "payment_terms_days", "status", "updated_at")

def _shape(df):
    """Rows that already carry effective_from / effective_to -> dim_customer's final columns."""
    return df.select(
        sk(F.col("customer_code"), F.col("effective_from").cast("string")).alias("customer_key"),   # one key PER VERSION
        "customer_id", "customer_code", "customer_name", "segment", "account_manager", "region", "payment_terms_days",
        "status", "effective_from", "effective_to", F.lit(True).alias("is_current"),
        F.xxhash64(*TRACKED).alias("row_hash"))      # fingerprint of the tracked columns: one number finds changes

def initial_load_customer(src):
    first = (src.withColumn("effective_from", F.to_date(F.lit(FAR_PAST))).withColumn("effective_to", F.to_date(F.lit(FAR_FUTURE))))
    shaped = _shape(first)
    unknown = spark.createDataFrame([(UNKNOWN_KEY, -1, "UNKNOWN", "Unknown customer", "Unknown", "Unknown", "Unknown", 0, "UNKNOWN",
                                      date(1900, 1, 1), date(9999, 12, 31), True, 0)], schema=shaped.schema)
    shaped.unionByName(unknown).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM_CUSTOMER)

def plan_changes(src, dim_df):
    """Work out what a merge must do. Pure DataFrame logic, no writes. Returns (staged, counts, type1_changes)."""
    current = dim_df.filter("is_current AND customer_id <> -1").select(
        "customer_id", F.col("row_hash").alias("cur_hash"), F.col("effective_from").alias("cur_from"))
    j = src.withColumn("row_hash", F.xxhash64(*TRACKED)).join(current, "customer_id", "left")
    is_new = F.col("cur_from").isNull()                                     # customer not in the dim yet
    is_changed = F.col("cur_from").isNotNull() & (F.col("row_hash") != F.col("cur_hash"))
    # Effective date of a change = the date the source stamped, but never before the day AFTER the current version
    # started, so a version can never have zero or negative length.
    change_from = F.greatest(F.to_date("updated_at"), F.date_add("cur_from", 1))
    versions = (j.filter(is_new | is_changed).withColumn("_is_change", is_changed)
                 .withColumn("effective_from", F.when(is_new, F.to_date(F.lit(FAR_PAST))).otherwise(change_from))
                 .withColumn("effective_to", F.to_date(F.lit(FAR_FUTURE))))
    counts = {"new_customers": versions.filter(~F.col("_is_change")).count(),
              "new_versions": versions.filter(F.col("_is_change")).count()}
    # Type 1 changes are counted against ALL versions of each customer.
    t1 = src.select("customer_id", *TYPE1)
    same = " AND ".join(f"t.{c} <=> s.{c}" for c in TYPE1)
    t1_changes = (dim_df.filter("customer_id <> -1").alias("t").join(t1.alias("s"), F.col("t.customer_id") == F.col("s.customer_id"))
                  .filter(F.expr(f"NOT ({same})")).select(F.col("s.customer_id")).distinct())
    counts["type1_updates"] = t1_changes.count()
    # The merge trick: ONE staged table. Rows with merge_key NULL never match -> they are INSERTED (new customers and
    # new versions). Rows with merge_key = customer_id match the CURRENT row -> it is CLOSED (is_current false).
    inserts = _shape(versions).withColumn("merge_key", F.lit(None).cast("int"))
    expires = _shape(versions.filter(F.col("_is_change"))).withColumn("merge_key", F.col("customer_id"))
    return inserts.unionByName(expires), counts, t1

def merge_changes(src):
    dim_df = spark.table(DIM_CUSTOMER)
    staged, counts, t1 = plan_changes(src, dim_df)
    if counts["new_customers"] + counts["new_versions"] > 0:
        staged.createOrReplaceTempView("_scd2_staged")
        cols = ", ".join(DIM_COLS)
        vals = ", ".join(f"s.{c}" for c in DIM_COLS)
        spark.sql(f"""MERGE INTO {DIM_CUSTOMER} t USING _scd2_staged s
                      ON t.customer_id = s.merge_key AND t.is_current = true
                      WHEN MATCHED THEN UPDATE SET is_current = false, effective_to = s.effective_from
                      WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})""")
    if counts["type1_updates"] > 0:
        t1.createOrReplaceTempView("_scd2_type1")
        same = " AND ".join(f"t.{c} <=> s.{c}" for c in TYPE1)
        sets = ", ".join(f"{c} = s.{c}" for c in TYPE1)
        spark.sql(f"""MERGE INTO {DIM_CUSTOMER} t USING _scd2_type1 s
                      ON t.customer_id = s.customer_id AND NOT ({same})
                      WHEN MATCHED THEN UPDATE SET {sets}""")
    return counts

def build_dim_customer():
    batch, src = new_batch("gold"), customer_source()
    if not spark.catalog.tableExists(DIM_CUSTOMER):
        initial_load_customer(src)
        print("dim_customer: initial load")
    else:
        print("dim_customer: merge ->", merge_changes(src))
    n_out = spark.table(DIM_CUSTOMER).count()
    log_gold(batch, "dim_customer", src.count(), n_out)
    print(f"gold dim_customer rows={n_out:,} (all versions + 1 unknown member)")

LATEST = [F.col("updated_at").desc(), F.col("_ingested_at").desc()]
def rebuild_silver_customers(until=None):
    rebuild_table("customers", spark.table(f"{BRONZE}.tms_customers"), ["customer_id"], LATEST,
                  customer_rules(), std_customers, until=until)

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# ANSI-safe parsing: try_to_timestamp / try_cast return NULL on bad input instead of raising. A NULL is then caught
# by a data-quality rule and the row is quarantined with a reason, which is exactly what we want.
def to_dt(column, fmt):
    return F.try_to_timestamp(F.col(column), F.lit(fmt)).cast("date")

def money(sql_expr):
    return F.expr(f"try_cast({sql_expr} AS decimal(12,2))")

META = ["_source_file", "_ingested_at"]

def map_col(col, mapping):
    """CASE WHEN col = k1 THEN v1 ... ; unmapped values become NULL (caught by a rule)."""
    expr = None
    for k, v in mapping.items():
        expr = F.when(col == k, v) if expr is None else expr.when(col == k, v)
    return expr

def parse_blz(df):
    """BlueLine: comma CSV, ISO dates, dashed refs, plain decimals."""
    return df.select(F.lit("BLZ").alias("carrier_code"), "invoice_no", to_dt("invoice_date", "yyyy-MM-dd").alias("invoice_date"),
                     F.col("shipment_ref").alias("_ref_raw"), F.upper(F.trim("charge_type")).alias("charge_type"),
                     money("amount").alias("amount"), F.lit(None).cast("decimal(12,2)").alias("gst_amount"),
                     F.upper(F.trim("currency")).alias("currency"), *META)

def parse_swf(df):
    """SwiftHaul: dd-MM-yyyy dates, refs without dashes, DECIMAL COMMA ("12345,67")."""
    desc = F.upper(F.trim("description"))
    return df.select(F.lit("SWF").alias("carrier_code"), F.col("inv_id").alias("invoice_no"),
                     to_dt("inv_date", "dd-MM-yyyy").alias("invoice_date"), F.col("ref").alias("_ref_raw"),
                     map_col(desc, {"BASE FREIGHT": "FREIGHT", "FUEL SURCHARGE": "FUEL", "DETENTION": "DETENTION"}).alias("charge_type"),
                     money("regexp_replace(amount_inr, ',', '.')").alias("amount"),
                     F.lit(None).cast("decimal(12,2)").alias("gst_amount"), F.lit("INR").alias("currency"), *META)

def parse_trk(df):
    """TruckNet: dd-MMM-yy dates, line-type codes, THOUSANDS separators ("12,345.67"), NET and GST split."""
    lt = F.upper(F.trim("line_type"))
    return df.select(F.lit("TRK").alias("carrier_code"), F.col("invoice_number").alias("invoice_no"),
                     to_dt("inv_date", "dd-MMM-yy").alias("invoice_date"), F.col("consignment").alias("_ref_raw"),
                     map_col(lt, {"F": "FREIGHT", "FS": "FUEL", "D": "DETENTION"}).alias("charge_type"),
                     money("regexp_replace(net_amt, ',', '')").alias("amount"),
                     money("regexp_replace(gst_amt, ',', '')").alias("gst_amount"), F.lit("INR").alias("currency"), *META)

def invoice_rules():
    """Applied AFTER the three layouts are parsed into one schema, so a NULL here means 'failed to parse'."""
    return [Rule("invoice_date_invalid", F.col("invoice_date").isNull(), "reject", "unparseable invoice date"),
            Rule("amount_invalid", F.col("amount").isNull(), "reject", "unparseable amount"),
            Rule("amount_non_positive", F.col("amount") <= 0, "reject", "invoice line must be > 0"),
            Rule("charge_type_unknown", F.col("charge_type").isNull(), "reject", "unmapped charge type"),
            orphan_rule("_known_ref", "orphan_no_shipment")]

def invoices_incremental(until=None):
    parts, new_wms = [], {}
    for code, parser in [("blz", parse_blz), ("swf", parse_swf), ("trk", parse_trk)]:
        inc, _ = bronze_increment(f"invoices_{code}", f"invoices_{code}", until)
        nw = max_ts(inc, "_ingested_at")
        if nw is not None:
            parts.append(parser(inc))
            new_wms[f"invoices_{code}"] = nw
    if not parts:
        print(f"silver {'carrier_invoice_lines':<22} nothing new since watermark")
        return
    batch = batch_id("carrier_invoice_lines", max(new_wms.values()))
    unified = parts[0]
    for p in parts[1:]:
        unified = unified.unionByName(p)
    n_in = unified.count()
    prepared = unified.withColumn("_ref_std", normalise_ref(F.col("_ref_raw")))
    # A duplicate = same carrier, invoice, shipment and charge type. Earliest ingestion wins.
    deduped = dedupe(prepared, ["carrier_code", "invoice_no", "_ref_std", "charge_type"],
                     [F.col("_ingested_at").asc(), F.col("_source_file").asc()])
    tagged = stage(evaluate(deduped.join(F.broadcast(known_refs()), "_ref_std", "left"), invoice_rules()), "invoice_lines")
    n_deduped = tagged.count()
    clean, rejected = split_tagged(tagged)
    clean = stamp(clean.select("carrier_code", "invoice_no", "invoice_date", F.col("_ref_std").alias("shipment_ref"),
                               "charge_type", "amount", "gst_amount", "currency", "_source_file",
                               F.col("_ingested_at").alias("_bronze_ingested_at"), "_dq_warn_reasons"), batch)
    merge_and_account("carrier_invoice_lines", batch, "carrier_invoices", n_in, n_deduped, clean,
                      rejected.drop("_known_ref", "_ref_std"),
                      ["carrier_code", "invoice_no", "shipment_ref", "charge_type"], upsert=False, new_wms=new_wms)

# ---------------------------------------------------------------- FX and fuel: small, rebuilt in full from append-only Bronze
LINEAGE_COLS = ["_source_file", "_ingested_at", "_source_system", "_source_object", "_batch_id", "_processing_date"]
drop_lineage = lambda df: df.drop(*LINEAGE_COLS)

def silver_reference(until=None):
    b = lambda n: spark.table(f"{BRONZE}.{n}")
    fx = (b("api_fx_rates").withColumn("rate_date", to_dt("date", "yyyy-MM-dd"))
          .withColumn("rate", F.expr("try_cast(rate AS decimal(10,4))")).drop("date"))
    rebuild_table("fx_rates", fx, ["rate_date", "base", "quote"], [F.col("_ingested_at").desc()],
                  [Rule("rate_date_invalid", F.col("rate_date").isNull(), "reject"),
                   # A USD/INR rate outside 60-110 is a feed error, not a market move.
                   Rule("rate_out_of_range", F.col("rate").isNull() | ~F.col("rate").between(60, 110), "reject")],
                  drop_lineage, source="reference_api", until=until)
    fuel = (b("api_fuel_index").withColumn("week_start", to_dt("week_start", "yyyy-MM-dd"))
            .withColumn("diesel_inr_per_litre", F.expr("try_cast(diesel_inr_per_litre AS decimal(8,2))")))
    rebuild_table("fuel_index", fuel, ["week_start"], [F.col("_ingested_at").desc()],
                  [Rule("week_start_invalid", F.col("week_start").isNull(), "reject"),
                   Rule("price_out_of_range", F.col("diesel_inr_per_litre").isNull() | ~F.col("diesel_inr_per_litre").between(60, 150), "reject")],
                  drop_lineage, source="reference_api", until=until)

def run_silver_3c(until=None):
    silver_reference(until)
    invoices_incremental(until)

# COMMAND ----------


spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# The scanner writes timestamps in three formats. dd/MM/yyyy is read day-first, never month-first: 04/03/2025 is 4 March.
TS_FORMATS = ["yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm", "yyyy-MM-dd'T'HH:mm:ss"]

def parse_event_ts(column="event_ts"):
    return F.coalesce(*[F.try_to_timestamp(F.col(column), F.lit(f)) for f in TS_FORMATS])

def scan_rules(valid_location_codes):
    ref, loc = F.col("shipment_ref"), F.col("location_code")
    return [
        not_null("event_type", "reject"),
        Rule("event_ts_unparseable", parse_event_ts().isNull(), "reject", "matches none of the 3 formats"),
        Rule("shipment_ref_bad_format", ref.isNull() | ~ref.rlike(r"^MFL-?\d{4}-?\d{7}$"), "reject",
             "not a shipment reference in any known format"),
        Rule("shipment_ref_nonstandard", ref.rlike(r"^MFL\d{4}\d{7}$"), "warn", "missing dashes"),
        # After upper/trim the code must exist in the location reference, else Gold uses "Unknown".
        Rule("location_unknown", loc.isNull() | ~F.upper(F.trim(loc)).isin(valid_location_codes), "warn",
             "location code not in reference data"),
        Rule("location_case", loc != F.upper(loc), "warn", "lowercase location code"),
        orphan_rule(),
    ]

def shape_scans(clean, batch):
    return stamp(clean.select(
        F.col("_ref_std").alias("shipment_ref"), "event_type",
        F.col("_ts").alias("event_ts"), F.to_date("_ts").alias("event_date"),
        F.col("_loc_std").alias("location_code"), "scan_source",
        F.col("_arrival_date").alias("arrival_date"),
        F.datediff(F.col("_arrival_date"), F.to_date("_ts")).alias("arrival_lag_days"),
        "_source_file", F.col("_ingested_at").alias("_bronze_ingested_at"), "_dq_warn_reasons"), batch)

def scans_incremental(until=None):
    inc, wm = bronze_increment("scans", "scans", until)
    new_wm = max_ts(inc, "_ingested_at")
    if new_wm is None:
        print(f"silver {'scans':<22} nothing new since {wm}")
        return
    batch = batch_id("scans", new_wm)
    n_in = inc.count()
    valid_locs = [r["location_code"] for r in spark.table(f"{SILVER}.locations").select("location_code").collect()]
    prepared = (inc.withColumn("_ref_std", normalise_ref(F.col("shipment_ref")))
                   .withColumn("_ts", parse_event_ts())
                   .withColumn("_loc_std", F.upper(F.trim("location_code"))))
    # The same physical scan can arrive in several files: keep the EARLIEST arrival.
    deduped = dedupe(prepared, ["_ref_std", "event_type", "_ts", "_loc_std"],
                     [F.col("_arrival_date").asc(), F.col("_ingested_at").asc()])
    tagged = stage(evaluate(deduped.join(F.broadcast(known_refs()), "_ref_std", "left"), scan_rules(valid_locs)), "scans")
    n_deduped = tagged.count()
    clean, rejected = split_tagged(tagged)
    merge_and_account("scans", batch, "scan_files", n_in, n_deduped, shape_scans(clean, batch),
                      rejected.drop("_known_ref", "_ref_std", "_ts", "_loc_std"),
                      ["shipment_ref", "event_type", "event_ts", "location_code"], upsert=False, new_wms={"scans": new_wm})

# COMMAND ----------

def run_silver_tms(until=None):
    b = lambda name: spark.table(f"{BRONZE}.{name}")
    latest = [F.col("updated_at").desc(), F.col("_ingested_at").desc()]
    rebuild_table("locations", b("tms_locations"), ["location_id"], [F.col("_ingested_at").desc()], [], std_locations, until=until)
    rebuild_table("carriers", b("tms_carriers"), ["carrier_id"], latest, [], std_carriers, until=until)
    rebuild_table("customers", b("tms_customers"), ["customer_id"], latest, customer_rules(), std_customers, until=until)
    upsert_table("shipments", "tms_shipments", "shipment_id", shipment_rules(), std_shipments, until=until)
    # Children are checked against the shipments that survived into Silver, AFTER this run's shipment merge.
    known = spark.table(f"{SILVER}.shipments").select("shipment_id").distinct().withColumn("_known_parent", F.lit(1))
    upsert_table("shipment_charges", "tms_shipment_charges", "charge_id", charge_rules(), std_charges, parents=known, until=until)
    upsert_table("claims", "tms_claims", "claim_id", claim_rules(), std_claims, parents=known, until=until)

# COMMAND ----------

API_COLS = {"fx_rates": "`date` STRING, base STRING, quote STRING, rate DOUBLE",
            "fuel_index": "week_start STRING, diesel_inr_per_litre DOUBLE"}
API_SELECT = {"fx_rates": "`date`, base, quote, CAST(rate AS DOUBLE) AS rate",
              "fuel_index": "week_start, CAST(diesel_inr_per_litre AS DOUBLE) AS diesel_inr_per_litre"}

def copy_api(batch):
    for name, cols in API_COLS.items():
        t = f"{BRONZE}.api_{name}"
        spark.sql(f"CREATE TABLE IF NOT EXISTS {t} ({cols}, _source_file STRING, _ingested_at TIMESTAMP, _batch_id STRING)")
        r = spark.sql(f"""
            COPY INTO {t}
            FROM (SELECT {API_SELECT[name]}, _metadata.file_path AS _source_file,
                         current_timestamp() AS _ingested_at, '{batch}' AS _batch_id
                  FROM '{LANDING}/api/{name}')
            FILEFORMAT = JSON
            FORMAT_OPTIONS ('multiLine' = 'true')""").first()
        print(f"api_{name:<18} +{r['num_inserted_rows']:>10,} rows (COPY INTO)")

def run_bronze(prefix):
    batch = new_batch(prefix)
    autoload("scans", "scans", "parquet", batch, "scan_files", schema=SCAN_SCHEMA)
    for code, (sep, cols) in CARRIERS.items():
        autoload(f"invoices_{code.lower()}", f"invoices/{code}", "csv", batch, f"carrier_{code.lower()}_invoices",
                 schema=StructType([StructField(c, StringType()) for c in cols]),
                 opts={"header": "true", "sep": sep})
    for f in dbutils.fs.ls(f"{LANDING}/tms"):          # TMS tables: schema INFERRED, new columns EVOLVE in
        t = f.name.rstrip("/")
        autoload(f"tms_{t}", f"tms/{t}", "parquet", batch, "tms_postgres")
    copy_api(batch)

def verify(parts):
    exp = {}
    for p in parts:
        for k, v in json.load(open(f"{LANDING}/manifest_{p}.json"))["rows"].items():
            exp[k] = exp.get(k, 0) + v
    ok = True
    for k, v in sorted(exp.items()):
        got = spark.table(f"{BRONZE}.{k}").count()
        ok &= (got == v)
        print(f"{'OK ' if got == v else 'BAD'} {k:<22} expected {v:>10,}  got {got:>10,}")
    print("ALL MATCH" if ok else "MISMATCH")

# COMMAND ----------

SCAN_SCHEMA = StructType([StructField(c, StringType()) for c in
                          ["shipment_ref", "event_type", "event_ts", "location_code", "scan_source"]])
CARRIERS = {   # carrier -> (delimiter, our column names in file order)
    "BLZ": (",", ["invoice_no", "invoice_date", "shipment_ref", "charge_type", "amount", "currency"]),
    "SWF": (";", ["inv_id", "inv_date", "ref", "description", "amount_inr"]),
    "TRK": (",", ["invoice_number", "inv_date", "consignment", "line_type", "net_amt", "gst_amt"]),
}

def new_batch(prefix):
    return f"{prefix}_{datetime.datetime.now(datetime.timezone.utc):%Y%m%dT%H%M%S}"

def autoload(name, subpath, fmt, batch, system, schema=None, opts=None):
    """Load only NEW files from landing/<subpath> into bronze.<name>. The checkpoint tracks which files are done."""
    table = f"{BRONZE}.{name}"
    before = spark.table(table).count() if spark.catalog.tableExists(table) else 0
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaLocation", f"{CKPT}/{name}/schema"))   # where inferred schema is stored
    for k, v in (opts or {}).items():
        reader = reader.option(k, v)
    if schema is not None:
        reader = reader.schema(schema)       # explicit schema: no inference, no evolution
    df = (reader.load(f"{LANDING}/{subpath}")
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_system", F.lit(system))
          .withColumn("_source_object", F.lit(subpath))
          .withColumn("_batch_id", F.lit(batch))
          .withColumn("_processing_date", F.current_date()))
    if name == "scans":   # arrival date lives in the file name; try_ keeps ANSI mode from raising on a bad name
        df = df.withColumn("_arrival_date", F.try_to_timestamp(
            F.regexp_extract("_source_file", r"scans_(\d{8})\.parquet", 1), F.lit("yyyyMMdd")).cast("date"))
    (df.writeStream.option("checkpointLocation", f"{CKPT}/{name}/chk")
       .option("mergeSchema", "true")
       .trigger(availableNow=True)           # process everything new, then stop (batch-style, serverless-safe)
       .toTable(table).awaitTermination())
    print(f"{name:<22} +{spark.table(table).count() - before:>10,} rows")

# COMMAND ----------

def param(name):
    return dbutils.widgets.get(name).strip()

def maybe_fail(step):
    """Failure drills. fail_at: the step always fails. fail_once_at: it fails on the FIRST attempt of a job run only,
    because a marker file records that the failure already happened, so the automatic retry succeeds."""
    if param("fail_at") == step:
        raise RuntimeError(f"simulated PERMANENT failure in step '{step}'")
    if param("fail_once_at") == step:
        os.makedirs(f"{ROOT}/_markers", exist_ok=True)
        marker = f"{ROOT}/_markers/{param('run_id')}_{step}"
        if not os.path.exists(marker):
            with open(marker, "w") as f:
                f.write("failed once")
            raise RuntimeError(f"simulated TRANSIENT failure in step '{step}' (attempt 1)")

def landing_zips():
    return sorted(f for f in os.listdir(ROOT) if f.startswith("meridian_landing_") and f.endswith(".zip"))

# ---------------------------------------------------------------- the steps (each one is a task in the job)
def step_check_landing():
    """Cheap checks first: fail fast, before any Spark work, if the Volume or the landing files are missing."""
    if not os.path.isdir(ROOT):
        raise FileNotFoundError(f"landing volume not reachable: {ROOT}")
    if not landing_zips() and not os.path.isdir(f"{LANDING}/scans"):
        raise FileNotFoundError("no landing zip and no unpacked landing files")
    print(f"landing OK: {len(landing_zips())} zip(s), unpacked folders: {sorted(os.listdir(LANDING))}")

def step_bronze():
    """Unpack any landing zip not yet unpacked (a marker file records each one, so a rerun is free), then ingest."""
    os.makedirs(f"{ROOT}/_unpacked", exist_ok=True)
    for z in landing_zips():
        if not os.path.exists(f"{ROOT}/_unpacked/{z}"):
            with zipfile.ZipFile(f"{ROOT}/{z}") as zf:
                zf.extractall(LANDING)
            with open(f"{ROOT}/_unpacked/{z}", "w") as f:
                f.write("done")
            print("unpacked", z)
    run_bronze("job")                  # Auto Loader + COPY INTO skip everything already loaded

def step_silver_tms():
    run_silver_tms()                   # locations, carriers, customers, shipments, charges, claims

def step_silver_scans():
    scans_incremental()

def step_silver_ref_invoices():
    run_silver_3c()                    # FX + fuel (rebuilt) and carrier invoice lines (incremental)

def step_gold():
    build_static_dims()                # tiny dimensions: rebuilt in full, keys are deterministic
    build_dim_customer()               # SCD2: merges only when a tracked column changed
    gold_incremental()                 # only the shipments whose inputs changed

def step_reconcile():
    """Run every check and report the outcome to the NEXT task through a task value. It does not raise on a break:
    the job's If/else task reads the value and decides which branch runs."""
    rc = reconcile()
    passed = not rc.failed
    try:
        dbutils.jobs.taskValues.set(key="passed", value=passed)
    except Exception as e:             # run by hand, outside a job, there is nowhere to store a task value
        print("(task value not stored:", str(e)[:80], ")")
    print("reconciliation passed:", passed)

def step_publish():
    publish_gold(param("run_id"))

def step_blocked():
    raise RuntimeError("reconciliation found breaks: NOTHING was published")

def step_reconcile_publish():          # fallback if your workspace has no If/else task: one task that gates itself
    reconcile_and_publish(param("run_id"))

STEPS = {"check_landing": step_check_landing, "bronze": step_bronze, "silver_tms": step_silver_tms,
         "silver_scans": step_silver_scans, "silver_ref_invoices": step_silver_ref_invoices, "gold": step_gold,
         "reconcile": step_reconcile, "publish": step_publish, "blocked": step_blocked,
         "reconcile_publish": step_reconcile_publish}

def run_step(step):
    if step not in STEPS:
        raise ValueError(f"unknown step '{step}'. Valid: {sorted(STEPS)}")
    maybe_fail(step)
    t0 = time.time()
    STEPS[step]()
    print(f"step '{step}' finished in {time.time() - t0:.0f}s")

# COMMAND ----------

run_step(param("step"))

# COMMAND ----------

   print([n for n in ["run_bronze", "run_silver_tms", "scans_incremental", "run_silver_3c", "build_static_dims",
                      "build_dim_customer", "gold_incremental", "reconcile", "publish_gold", "run_step"] if n not in globals()])