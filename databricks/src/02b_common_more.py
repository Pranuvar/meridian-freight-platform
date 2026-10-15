# Databricks notebook source
def merge_and_account(table, batch, source, n_in, n_deduped, clean, rejected, key_cols, upsert=False, new_wms=None):
    """The shared tail of every Silver job: MERGE the clean rows, quarantine the rejects, prove nothing was lost,
    and only THEN advance the watermark(s)."""
    n_clean, n_rej = clean.count(), rejected.count()
    target = f"{SILVER}.{table}"
    ensure_table(clean, target)
    before = spark.table(target).count()
    inserted, updated = merge_into(target, clean, key_cols, upsert) if n_clean else (0, 0)
    after = spark.table(target).count()
    clear_quarantine(table, batch)
    if n_rej:
        write_quarantine(rejected, table, source, batch)
    ignored = n_clean - inserted - updated                 # key already present: safely skipped
    n_dups = (n_in - n_deduped) + ignored
    ok = n_deduped == n_clean + n_rej and ignored >= 0 and after - before == inserted
    print(f"silver {table:<22} in={n_in:>9,} = inserted={inserted:>9,} + updated={updated:>7,} + "
          f"rejected={n_rej:>7,} + duplicates={n_dups:>8,}  {'PASS' if ok else 'FAIL'}")
    log_run(batch, table, n_in, inserted + updated, n_rej, n_dups)
    if not ok:
        raise RuntimeError(f"{table}: accounting failed, watermark NOT advanced")
    for k, v in (new_wms or {}).items():
        set_ts(f"silver.{k}", v)

def known_refs():
    """Every shipment reference that exists in BRONZE (so a scan of a shipment that was rejected for a bad weight
    still counts as known, not as an orphan)."""
    return (spark.table(f"{BRONZE}.tms_shipments").select(F.col("shipment_ref").alias("_ref_std")).distinct()
                 .withColumn("_known_ref", F.lit(1)))

def normalise_ref(col):
    """MFL20250000123 or MFL-2025-0000123 -> MFL-2025-0000123 (the standard form)."""
    return F.regexp_replace(col, r"^MFL-?(\d{4})-?(\d{7})$", "MFL-$1-$2")