# Databricks notebook source
import re, json, datetime
from dataclasses import dataclass
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# ---------------------------------------------------------------- names and settings
catalog = spark.sql("SELECT current_catalog()").first()[0]
BRONZE, SILVER, GOLD, OPS = (f"`{catalog}`.meridian_{layer}" for layer in ["bronze", "silver", "gold", "ops"])
STATE, RUN_LOG, QUARANTINE = f"{OPS}.state", f"{OPS}.run_log", f"{OPS}.rejected_records"
FMT = "yyyy-MM-dd HH:mm:ss.SSSSSS"      # microsecond precision, so a watermark never rounds a timestamp

def new_batch(prefix):
    return f"{prefix}_{datetime.datetime.now(datetime.timezone.utc):%Y%m%dT%H%M%S}"

# ---------------------------------------------------------------- watermarks (a small Delta table, updated with MERGE)
spark.sql(f"CREATE TABLE IF NOT EXISTS {STATE} (key STRING, ts TIMESTAMP)")

def get_ts(key):
    """Watermark for `key` as a string. Never set = the beginning of time, so a first run processes everything."""
    row = spark.table(STATE).filter(F.col("key") == key).select(F.date_format("ts", FMT).alias("s")).first()
    return row["s"] if row else "1900-01-01 00:00:00.000000"

def set_ts(key, ts_str):
    spark.sql(f"""MERGE INTO {STATE} t
                  USING (SELECT '{key}' AS key, CAST('{ts_str}' AS TIMESTAMP) AS ts) s ON t.key = s.key
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")

def max_ts(df, column):
    return df.agg(F.date_format(F.max(column), FMT)).first()[0]       # None for an empty DataFrame

# ---------------------------------------------------------------- data-quality rules (a Rule describes what a BAD row looks like)
@dataclass(frozen=True)
class Rule:
    name: str
    fail: Column                 # TRUE when the row VIOLATES the rule
    severity: str = "reject"     # "reject" -> quarantined | "warn" -> row kept, problem recorded
    description: str = ""

def not_null(column, severity="reject"):
    c = F.col(column)
    return Rule(f"{column}_missing", c.isNull() | (F.trim(c.cast("string")) == ""), severity, f"{column} is null or blank")

def not_in_set(column, allowed, severity="reject"):
    c = F.col(column)
    return Rule(f"{column}_invalid", c.isNotNull() & ~c.isin(allowed), severity, f"{column} not in {allowed}")

def orphan_rule(marker="_known_ref", name="orphan_no_shipment"):
    """The job left-joins a table of known keys and adds `marker` (NULL when there was no match)."""
    return Rule(name, F.col(marker).isNull(), "reject", "no matching parent record")

def shipment_rules():
    return [Rule("promised_before_booked", F.col("promised_delivery_at") < F.col("booked_at"), "reject", "SLA promise earlier than booking"),
            Rule("weight_non_positive", F.col("weight_kg") <= 0, "reject", "impossible weight"),
            not_null("weight_kg", "warn"), not_null("quoted_carrier_cost", "warn")]

def customer_rules():
    return [not_in_set("segment", ["Enterprise", "Mid-Market", "SME"], "warn"), not_in_set("status", ["ACTIVE", "INACTIVE"], "reject")]

def charge_rules():
    return [Rule("amount_non_positive", F.col("amount") <= 0, "reject", "customer charge must be > 0"),
            not_null("currency", "reject"), not_in_set("currency", ["INR", "USD"], "reject"),
            orphan_rule("_known_parent", "parent_shipment_missing")]

def claim_rules():
    return [Rule("claim_amount_non_positive", F.col("claim_amount") <= 0, "reject"),
            not_in_set("claim_type", ["DAMAGE", "LOSS", "DELAY"], "reject"),
            not_in_set("status", ["OPEN", "SETTLED", "REJECTED"], "reject"),
            orphan_rule("_known_parent", "parent_shipment_missing")]

# ---------------------------------------------------------------- the quality engine
def evaluate(df, rules):
    """Add two array columns naming the rules each row broke. F.when(...) is NULL when a rule passes; the leading
    NULL keeps the array typed even when a table has no rules of that severity; F.filter drops the NULLs."""
    def tag(severity):
        cols = [F.when(r.fail, F.lit(r.name)) for r in rules if r.severity == severity]
        return F.filter(F.array(F.lit(None).cast("string"), *cols), lambda x: x.isNotNull())
    return df.withColumn("_dq_reject_reasons", tag("reject")).withColumn("_dq_warn_reasons", tag("warn"))

def split_tagged(tagged):
    """(clean, rejected) from an already-evaluated DataFrame. Warnings ride along on clean rows."""
    bad = F.size("_dq_reject_reasons") > 0
    return tagged.filter(~bad).drop("_dq_reject_reasons"), tagged.filter(bad)

def write_quarantine(rejected, table, source, batch_id):
    """Rejected rows + reasons. The original row is kept as JSON so ONE table holds rows from every source table."""
    cols = [c for c in rejected.columns if not c.startswith("_dq_")]
    (rejected.select(F.lit(table).alias("table_name"), F.lit(source).alias("source_system"),
                     F.lit(batch_id).alias("batch_id"), F.current_timestamp().alias("quarantined_at"),
                     F.concat_ws("|", "_dq_reject_reasons").alias("reject_reasons"),
                     F.to_json(F.struct(*[F.col(c) for c in cols])).alias("record"))
             .write.mode("append").saveAsTable(QUARANTINE))

def clear_quarantine(table, batch=None):
    """A retry replaces its own quarantine rows instead of duplicating them."""
    if spark.catalog.tableExists(QUARANTINE):
        cond = f"table_name = '{table}'" + (f" AND batch_id = '{batch}'" if batch else "")
        spark.sql(f"DELETE FROM {QUARANTINE} WHERE {cond}")

# ---------------------------------------------------------------- shared plumbing
def dedupe(df, keys, order_by):
    """ONE row per key: the first after sorting by order_by. row_number() over a window = a shuffle by key."""
    w = Window.partitionBy(*keys).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

def stage(df, name):
    """Serverless has no cache()/persist(). The replacement: write the expensive result ONCE to a Delta table and
    read it back, so counts, the merge and the quarantine all reuse it instead of recomputing the window shuffle."""
    t = f"{OPS}.stg_{name}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t)
    return spark.table(t)

def bounded(df, until):
    """Optional ceiling on Bronze arrival time: lets us replay 'the initial load' even though June is already in Bronze."""
    return df if until is None else df.filter(F.col("_ingested_at") <= F.lit(until).cast("timestamp"))

def bronze_increment(bronze_name, layer_key, until=None):
    """Bronze rows that ARRIVED after the layer watermark. Arrival time, not event time: a late record has an old
    business timestamp but a new _ingested_at."""
    wm = get_ts(f"silver.{layer_key}")
    b = spark.table(f"{BRONZE}.{bronze_name}")
    return bounded(b.filter(F.col("_ingested_at") > F.lit(wm).cast("timestamp")), until), wm

def bronze_ceiling(prefix):
    """Latest _ingested_at over every Bronze table, for batches whose id starts with `prefix` (e.g. 'initial')."""
    best = None
    for r in spark.sql(f"SHOW TABLES IN {BRONZE}").collect():
        t = spark.table(f"{BRONZE}.{r['tableName']}")
        if "_batch_id" in t.columns:
            v = t.filter(F.col("_batch_id").startswith(prefix)).agg(F.date_format(F.max("_ingested_at"), FMT)).first()[0]
            best = v if best is None or (v is not None and v > best) else best
    return best

def batch_id(table, new_wm):
    """Deterministic: the same increment always gets the same id, so a retry replaces its own quarantine rows."""
    return f"inc_{table}_{re.sub(r'[^0-9]', '', new_wm)}"

def ensure_table(df, target):
    """Create an EMPTY Silver table with the right schema if it is missing, so the first load and every later
    load take the same MERGE path."""
    if not spark.catalog.tableExists(target):
        df.limit(0).write.saveAsTable(target)

def merge_into(target, df, key_cols, upsert):
    """upsert=True : UPDATE when the incoming row is STRICTLY newer, INSERT new keys   (shipments, charges, claims)
       upsert=False: INSERT keys not present yet, ignore the rest                      (scans, invoice lines)
       Equal or older versions are left alone, which is what makes replays harmless. Returns (inserted, updated)."""
    df.createOrReplaceTempView("_merge_src")
    op = "=" if upsert else "<=>"            # <=> is null-safe equality: NULL matches NULL instead of never matching
    on = " AND ".join(f"t.{c} {op} s.{c}" for c in key_cols)
    when_matched = "WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *\n" if upsert else ""
    r = spark.sql(f"MERGE INTO {target} t USING _merge_src s ON {on}\n{when_matched}WHEN NOT MATCHED THEN INSERT *").first()
    return int(r["num_inserted_rows"]), int(r["num_updated_rows"])

RUN_LOG_SCHEMA = StructType([StructField("run_ts", TimestampType()), StructField("batch_id", StringType()),
    StructField("layer", StringType()), StructField("table_name", StringType()), StructField("records_in", LongType()),
    StructField("records_out", LongType()), StructField("records_rejected", LongType()), StructField("duplicates_removed", LongType())])

def log_run(batch, table, n_in, n_out, n_rej, n_dups):
    row = [(datetime.datetime.now(datetime.timezone.utc), batch, "silver", table, int(n_in), int(n_out), int(n_rej), int(n_dups))]
    spark.createDataFrame(row, RUN_LOG_SCHEMA).write.mode("append").saveAsTable(RUN_LOG)

def stamp(df, batch):
    return df.withColumn("_silver_batch_id", F.lit(batch)).withColumn("_silver_processed_at", F.current_timestamp())

# ---------------------------------------------------------------- standardisation per table
def std_locations(df): return df.withColumn("location_code", F.upper(F.trim("location_code"))).withColumn("region", F.trim("region"))
def std_carriers(df): return df.withColumn("carrier_code", F.upper(F.trim("carrier_code")))
def std_customers(df):
    seg = F.upper(F.trim("segment"))
    return df.withColumn("segment", F.when(seg == "ENTERPRISE", "Enterprise").when(seg == "MID-MARKET", "Mid-Market")
                                     .when(seg == "SME", "SME").otherwise(F.col("segment")))   # unknown values stay visible
def std_shipments(df):
    return (df.withColumn("status", F.upper(F.trim("status"))).withColumn("service_level", F.upper(F.trim("service_level")))
              .withColumn("currency", F.upper(F.trim("currency"))))
def std_charges(df): return df.withColumn("charge_type", F.upper(F.trim("charge_type"))).withColumn("currency", F.upper(F.trim("currency")))
def std_claims(df): return df.withColumn("claim_type", F.upper(F.trim("claim_type"))).withColumn("status", F.upper(F.trim("status")))

# ---------------------------------------------------------------- the two Silver patterns
def rebuild_table(table, df, keys, order_by, rules, std, source="tms_postgres", until=None):
    """Small tables: rebuild the whole Silver table from append-only Bronze (dedupe -> validate -> overwrite)."""
    df = bounded(df, until)
    batch = new_batch("silver")
    n_in = df.count()
    tagged = stage(evaluate(dedupe(df, keys, order_by), rules), table)
    n_deduped = tagged.count()
    clean, rejected = split_tagged(tagged)
    target = f"{SILVER}.{table}"
    stamp(std(clean), batch).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    n_out, n_rej, n_dups = spark.table(target).count(), rejected.count(), n_in - n_deduped
    clear_quarantine(table)
    if n_rej:
        write_quarantine(rejected, table, source, batch)
    ok = n_in == n_out + n_rej + n_dups
    print(f"silver {table:<22} in={n_in:>8,} = out={n_out:>8,} + rejected={n_rej:>6,} + duplicates={n_dups:>7,}  {'PASS' if ok else 'FAIL'}")
    log_run(batch, table, n_in, n_out, n_rej, n_dups)
    if not ok:
        raise RuntimeError(f"{table}: accounting failed")

def upsert_table(table, bronze_name, key, rules, std, parents=None, until=None):
    """Big tables: only Bronze rows newer than the layer watermark -> dedupe -> validate -> MERGE.
    The watermark moves LAST, and only if the books balance."""
    inc, wm = bronze_increment(bronze_name, table, until)
    new_wm = max_ts(inc, "_ingested_at")
    if new_wm is None:
        print(f"silver {table:<22} nothing new since {wm}")
        return
    batch = batch_id(table, new_wm)
    n_in = inc.count()
    # Several versions of one key (or a re-read after a crash) collapse to the NEWEST one.
    deduped = dedupe(inc, [key], [F.col("updated_at").desc(), F.col("_ingested_at").desc()])
    if parents is not None:      # broadcast: the small parent table is copied to every task, so the big side never shuffles
        deduped = deduped.join(F.broadcast(parents), "shipment_id", "left")
    tagged = stage(evaluate(deduped, rules), table)
    n_deduped = tagged.count()
    clean, rejected = split_tagged(tagged)
    helper = [c for c in ["_known_parent"] if c in tagged.columns]
    clean, rejected = stamp(std(clean.drop(*helper)), batch), rejected.drop(*helper)
    n_clean, n_rej = clean.count(), rejected.count()

    target = f"{SILVER}.{table}"
    ensure_table(clean, target)
    before = spark.table(target).count()
    inserted, updated = merge_into(target, clean, [key], upsert=True) if n_clean else (0, 0)
    after = spark.table(target).count()
    clear_quarantine(table, batch)
    if n_rej:
        write_quarantine(rejected, table, "tms_postgres", batch)

    ignored = n_clean - inserted - updated                 # already present, or an older version: safely skipped
    n_dups = (n_in - n_deduped) + ignored
    ok = n_deduped == n_clean + n_rej and ignored >= 0 and after - before == inserted
    print(f"silver {table:<22} in={n_in:>8,} = inserted={inserted:>7,} + updated={updated:>7,} + "
          f"rejected={n_rej:>6,} + duplicates/stale={n_dups:>7,}  {'PASS' if ok else 'FAIL'}")
    log_run(batch, table, n_in, inserted + updated, n_rej, n_dups)
    if not ok:
        raise RuntimeError(f"{table}: accounting failed, watermark NOT advanced")
    set_ts(f"silver.{table}", new_wm)