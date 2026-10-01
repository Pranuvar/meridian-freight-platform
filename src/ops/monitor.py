"""Pipeline health report: what a production data engineer watches.
  python -m src.ops.monitor [--asof 2026-06-08]
Freshness is measured in BUSINESS time against --asof (default: now). The simulated business stopped on 7 June, so
against today's date everything is correctly STALE. Use --asof 2026-06-08 to see a healthy report."""
import argparse
import json
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.ops.health import freshness, volume_anomaly
from src.ops.task import EVENTS
from src.silver.common import silver_path
from src.utils.config import CONTROL
from src.utils.spark import get_spark
from src.utils.state import get_ts


def delta(spark, path):
    return spark.read.format("delta").load(str(path)) if DeltaTable.isDeltaTable(spark, str(path)) else None


# ---------------------------------------------------------------- 1. what did the last run do?
def section_runs(issues):
    print("\n--- 1. latest pipeline run ---")
    events = [json.loads(line) for line in EVENTS.read_text().splitlines() if line.strip()] if EVENTS.exists() else []
    if not events:
        print("no runs recorded yet (run: python -m src.ops.run_local daily --asof 2026-06-07 --fast)")
        issues.append("no pipeline run has ever been recorded")
        return
    last_run = max(events, key=lambda e: e["ended_at"])["run_id"]
    rows = sorted((e for e in events if e["run_id"] == last_run), key=lambda e: e["ended_at"])
    final, attempts = {}, {}
    for e in rows:                                   # a retried task has several events: keep the last one
        final[e["task"]] = e
        attempts[e["task"]] = max(attempts.get(e["task"], 0), e.get("attempt") or 0)
    print(f"run {last_run}")
    print(f"{'task':<26}{'status':<10}{'attempts':>9}{'seconds':>9}  ended (UTC)")
    for t, e in final.items():
        print(f"{t:<26}{e['status']:<10}{attempts[t]:>9}{e['duration_s']:>9}  {e['ended_at']}")
    bad = [t for t, e in final.items() if e["status"] != "SUCCESS"]
    from src.ops import pipeline_spec as spec
    expected = [t.name for t in (spec.BACKFILL if "backfill_facts" in final else spec.DAILY)]
    waiting = [t for t in expected if t not in final]
    if waiting:     # no event = still running, still waiting, or never started (Airflow's upstream_failed writes none)
        print("\nno result recorded yet for: " + ", ".join(waiting))
        issues.append(f"last run {last_run} incomplete: no result for {', '.join(waiting)}")
    if bad:
        issues.append(f"last run {last_run}: not successful -> {', '.join(bad)}")
        first = next(e for e in final.values() if e["status"] == "FAILED") if any(
            e["status"] == "FAILED" for e in final.values()) else None
        if first:
            print(f"\nwhy {first['task']} failed (last lines):\n  " + "\n  ".join(first["error_tail"].splitlines()[-6:]))
    pubs = [e for e in events if e["task"] == "publish" and e["status"] == "SUCCESS"]
    print("\nlast successful publish:", max(e["ended_at"] for e in pubs) if pubs else "never")


# ---------------------------------------------------------------- 2. how fresh is the data?
def section_freshness(spark, now, issues):
    print("\n--- 2. source freshness (business time) ---")

    def newest(df, expr):
        return df.agg(F.date_format(expr, "yyyy-MM-dd HH:mm:ss")).first()[0]

    scans, inv, fx = (delta(spark, silver_path(n)) for n in ("scans", "carrier_invoice_lines", "fx_rates"))
    # (label, newest business timestamp as text, limit in hours)
    metrics = [
        ("newest shipment update in Postgres", get_ts(spark, "tms.shipments"), 36),
        ("newest scan event", newest(scans, F.max("event_ts")) if scans is not None else None, 36),
        ("newest carrier invoice date", newest(inv, F.max("invoice_date")) if inv is not None else None, 24 * 35),
        ("newest FX rate (weekdays only)", newest(fx, F.max("rate_date")) if fx is not None else None, 96),
    ]
    print(f"{'metric':<38}{'newest':<22}{'hours behind':>14}{'limit (h)':>11}  status")
    for label, ts, limit in metrics:
        if ts is None:
            print(f"{label:<38}{'never':<22}{'':>14}{limit:>11}  STALE")
            issues.append(f"{label}: no data")
            continue
        newest_dt = datetime.fromisoformat(ts.replace(" ", "T"))
        hours = (now - newest_dt).total_seconds() / 3600
        status = freshness(hours, limit)
        print(f"{label:<38}{ts[:19]:<22}{hours:>14,.1f}{limit:>11}  {status}")
        if status != "OK":
            issues.append(f"{label} is {hours:,.0f}h behind (limit {limit}h)")


# ---------------------------------------------------------------- 3. what did the last loads do, and is the volume normal?
def section_loads(spark, issues):
    print("\n--- 3. latest Silver load per table ---")
    log = delta(spark, CONTROL / "run_log")
    if log is None:
        print("no run log yet")
        return
    silver = log.filter("layer = 'silver'")
    w = Window.partitionBy("table_name").orderBy(F.desc("run_ts"))
    latest = silver.withColumn("rn", F.row_number().over(w)).filter("rn = 1").orderBy("table_name").collect()
    print(f"{'table':<24}{'kind':<13}{'in':>10}{'out':>10}{'rejected':>10}{'duplicates':>12}{'reject %':>10}")
    for r in latest:
        pct = r["records_rejected"] / r["records_in"] * 100 if r["records_in"] else 0.0
        kind = "incremental" if r["batch_id"].startswith("inc_") else "full build"
        print(f"{r['table_name']:<24}{kind:<13}{r['records_in']:>10,}{r['records_out']:>10,}"
              f"{r['records_rejected']:>10,}{r['duplicates_removed']:>12,}{pct:>10.2f}")
        if pct > 5:
            issues.append(f"{r['table_name']}: {pct:.1f}% of the last load was rejected")

    print("\n--- volume check: latest incremental load vs the median of earlier ones ---")
    hist = {}
    for r in (silver.filter(F.col("batch_id").startswith("inc_")).select("table_name", "records_in")
              .orderBy("run_ts").collect()):
        hist.setdefault(r["table_name"], []).append(r["records_in"])
    for table, values in sorted(hist.items()):
        status, detail = volume_anomaly(values[:-1], values[-1])
        print(f"  {table:<24}{status:<22}{detail}")
        if status == "ANOMALY":
            issues.append(f"{table}: unusual load volume ({detail})")


# ---------------------------------------------------------------- 4. data quality hot spots
def section_quality(spark):
    print("\n--- 4. data-quality hot spots (latest Bronze profile) ---")
    dq = delta(spark, CONTROL / "dq_run_log")
    if dq is None:
        print("no quality profile yet (python -m src.quality.profile_bronze)")
        return
    batch = dq.orderBy(F.desc("run_ts")).first()["batch_id"]
    top = (dq.filter((F.col("batch_id") == batch) & (F.col("failed_count") > 0) & F.col("severity").isin("reject", "warn"))
           .orderBy(F.desc("failed_pct")).limit(6).collect())
    print(f"profile batch {batch}")
    for r in top:
        print(f"  {r['table_name']:<16}{r['rule_name']:<30}{r['severity']:<8}{r['failed_count']:>9,}  {r['failed_pct']:.2f}%")


# ---------------------------------------------------------------- 5. latest reconciliation
def section_recon(spark, issues):
    print("\n--- 5. latest reconciliation ---")
    rec = delta(spark, CONTROL / "recon_results")
    if rec is None:
        print("no reconciliation has run yet (python -m src.reconciliation.run)")
        issues.append("reconciliation has never run")
        return
    last = rec.agg(F.max("run_ts")).first()[0]
    shown = rec.agg(F.date_format(F.max("run_ts"), "yyyy-MM-dd HH:mm:ss")).first()[0]   # formatted inside Spark, in UTC
    rows = rec.filter(F.col("run_ts") == last)
    counts = {r["status"]: r["count"] for r in rows.groupBy("status").count().collect()}
    print(f"run at {shown} UTC: " + "   ".join(f"{s}: {counts.get(s, 0)}" for s in ("PASS", "EXPLAINED", "INFO", "FAIL")))
    for r in rows.filter("status = 'FAIL'").collect():
        print(f"  FAIL  {r['check_name']}: difference {r['difference']}")
        issues.append(f"reconciliation FAIL: {r['check_name']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", help="evaluate freshness as of this date/time, UTC (default: now)")
    args = ap.parse_args()
    now = datetime.fromisoformat(args.asof) if args.asof else datetime.now(timezone.utc).replace(tzinfo=None)
    issues = []
    print(f"\nPIPELINE HEALTH REPORT   as of {now:%Y-%m-%d %H:%M} UTC")
    section_runs(issues)
    spark = get_spark("monitor")
    section_freshness(spark, now, issues)
    section_loads(spark, issues)
    section_quality(spark)
    section_recon(spark, issues)
    spark.stop()
    print("\nPIPELINE HEALTH: " + ("OK" if not issues else f"ATTENTION ({len(issues)} issue(s))"))
    for i in issues:
        print("  -", i)


if __name__ == "__main__":
    main()
