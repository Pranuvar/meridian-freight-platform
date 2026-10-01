"""Reconciliation part 1: does every record and every rupee survive Postgres/files -> Bronze -> Silver (+ quarantine)?"""
from decimal import Decimal

from pyspark.sql import functions as F

from src.quality.rules import normalise_ref, parse_event_ts
from src.silver.build_invoices import parse_blz, parse_swf, parse_trk
from src.silver.common import silver_path
from src.utils.config import PG_PROPS, PG_URL, QUARANTINE, bronze_path

ENTITIES = [("customers", "customer_id"), ("shipments", "shipment_id"),
            ("shipment_charges", "charge_id"), ("claims", "claim_id")]
ROW_TOLERANCE = 50   # see ledger(): a handful of rows can legitimately fall between two accounting buckets


def read_delta(spark, path):
    return spark.read.format("delta").load(path)


def pg_query(spark, sql):
    """The SQL runs INSIDE Postgres, so only the small answer crosses the wire."""
    return spark.read.jdbc(PG_URL, f"({sql}) AS q", properties=PG_PROPS)


def quarantine(spark, table):
    return read_delta(spark, str(QUARANTINE / "rejected_records")).filter(F.col("table_name") == table)


def json_field(col, name):
    """Quarantine keeps each rejected record as JSON text; this reads one field back out of it."""
    return F.get_json_object(col, f"$.{name}")


def reasons(qt, key):
    """Reject reasons by DISTINCT key (a row rejected in two batches counts once)."""
    rows = (qt.select(json_field(qt["record"], key).alias("k"), "reject_reasons").distinct()
            .groupBy("reject_reasons").count().collect())
    return "; ".join(f"{r['reject_reasons']} = {r['count']:,}" for r in sorted(rows, key=lambda r: -r["count"])) or "none"


def reason_rows(qt):
    rows = qt.groupBy("reject_reasons").count().collect()
    return "; ".join(f"{r['reject_reasons']} = {r['count']:,}" for r in sorted(rows, key=lambda r: -r["count"])) or "none"


# ---------------------------------------------------------------- 1. entities, key by key
def entity_flow(spark, rc):
    for table, key in ENTITIES:
        source = pg_query(spark, f"SELECT COUNT(*) AS n FROM tms.{table}").first()["n"]
        b_keys = read_delta(spark, bronze_path(f"tms_{table}")).select(F.col(key).cast("long").alias(key)).distinct()
        s_keys = read_delta(spark, silver_path(table)).select(F.col(key).cast("long").alias(key)).distinct()
        qt = quarantine(spark, table)
        q_keys = qt.select(json_field(qt["record"], key).cast("long").alias(key)).distinct()

        n_b, n_s = b_keys.count(), s_keys.count()
        only_q = q_keys.join(s_keys, key, "left_anti").count()                         # rejected and NOT in Silver
        both = s_keys.join(q_keys, key, "inner").count()
        neither = b_keys.join(s_keys.unionByName(q_keys), key, "left_anti").count()    # lost without trace

        rc.equal(f"{table}: record count", "Postgres", "Bronze", source, n_b,
                 note="distinct keys: Bronze keeps every version of a row, and the crash test wrote some twice")
        rc.explained(f"{table}: rows missing from Silver", "Postgres", "quarantine", source - n_s, only_q,
                     note=reasons(qt, key))
        rc.equal(f"{table}: keys in neither Silver nor quarantine", "Bronze", "Silver+quarantine", 0, neither)
        rc.info(f"{table}: keys in BOTH Silver and quarantine", "Silver", "quarantine", both,
                note="a row rejected once that later arrived valid; expected 0")


# ---------------------------------------------------------------- 2. charge amounts, by currency
def charge_amounts(spark, rc):
    def by_currency(df):
        rows = df.groupBy("currency").agg(F.count("*").alias("n"), F.sum("amount").alias("s")).collect()
        return {r["currency"]: (r["n"], r["s"]) for r in rows}

    # ::numeric(20,2) gives Spark a bounded decimal type instead of Postgres's unbounded numeric.
    src = {r["currency"]: (r["n"], r["s"]) for r in pg_query(
        spark, "SELECT currency, COUNT(*) AS n, SUM(amount)::numeric(20,2) AS s "
               "FROM tms.shipment_charges GROUP BY currency").collect()}
    silver = by_currency(read_delta(spark, silver_path("shipment_charges")))
    qt = quarantine(spark, "shipment_charges")
    quarantined = by_currency(qt.select(
        json_field(qt["record"], "charge_id").cast("long").alias("id"),
        json_field(qt["record"], "currency").alias("currency"),
        json_field(qt["record"], "amount").cast("decimal(14,2)").alias("amount")).distinct())

    zero = (0, Decimal("0.00"))
    for cur in sorted(src):
        (sn, ss), (qn, qs) = silver.get(cur, zero), quarantined.get(cur, zero)
        rc.equal(f"charges {cur}: row count", "Postgres", "Silver+quarantine", src[cur][0], sn + qn)
        rc.equal(f"charges {cur}: amount", "Postgres", "Silver+quarantine", src[cur][1], ss + qs,
                 note=f"native {cur}, exact to the smallest unit")


# ---------------------------------------------------------------- 3. event and line ledgers
def ledger(rc, name, n_bronze, n_silver, n_quar, n_groups, lo, hi, note):
    """Bronze rows = Silver + quarantine + duplicates. Duplicates are RECOMPUTED from Bronze (rows minus distinct
    business keys), not read from the pipeline's own log, so the check is independent of the code it verifies.
    ROW_TOLERANCE: a duplicate pair split across the initial load and an increment whose surviving row is
    rejected is counted as two rejects, not one reject plus one duplicate. That can move a few rows between
    buckets, never change the total."""
    dup_independent = n_bronze - n_groups
    rc.equal(f"{name}: Bronze = Silver + quarantine + duplicates", "Bronze", "Silver+quarantine+dups",
             n_bronze, n_silver + n_quar + dup_independent, tol=ROW_TOLERANCE,
             note=f"{n_bronze:,} = {n_silver:,} + {n_quar:,} + {dup_independent:,} "
                  f"(pipeline's own residual: {n_bronze - n_silver - n_quar:,})")
    rc.band(f"{name}: duplicate rate %", "Bronze", "-", round(dup_independent / n_bronze * 100, 3), lo, hi, note=note)


def scans_flow(spark, rc):
    bronze = read_delta(spark, bronze_path("scans"))
    prepared = (bronze.withColumn("_ref_std", normalise_ref(F.col("shipment_ref")))
                      .withColumn("_ts", parse_event_ts())
                      .withColumn("_loc_std", F.upper(F.trim("location_code"))))
    # groupBy treats NULL as a value (unlike count(distinct)), exactly like the Silver window does.
    groups = prepared.groupBy("_ref_std", "event_type", "_ts", "_loc_std").count().count()
    qt = quarantine(spark, "scans")
    ledger(rc, "scans", bronze.count(), read_delta(spark, silver_path("scans")).count(), qt.count(), groups,
           1.5, 2.5, "scanners re-send about 2% of scans")
    rc.info("scans: quarantine reasons", "Silver", "quarantine", qt.count(), note=reason_rows(qt))


def invoices_flow(spark, rc):
    b = lambda n: read_delta(spark, bronze_path(n))      # noqa: E731
    parsed = (parse_blz(b("invoices_blz")).unionByName(parse_swf(b("invoices_swf")))
              .unionByName(parse_trk(b("invoices_trk"))))
    prepared = parsed.withColumn("_ref_std", normalise_ref(F.col("_ref_raw")))
    groups = prepared.groupBy("carrier_code", "invoice_no", "_ref_std", "charge_type").count().count()
    qt = quarantine(spark, "carrier_invoice_lines")
    ledger(rc, "invoice lines", parsed.count(), read_delta(spark, silver_path("carrier_invoice_lines")).count(),
           qt.count(), groups, 2.5, 3.5, "carriers re-send about 3% of lines")
    rc.info("invoice lines: quarantine reasons", "Silver", "quarantine", qt.count(), note=reason_rows(qt))
