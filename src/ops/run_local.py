"""Run the pipeline the way an orchestrator would: dependency order, retries with exponential backoff, and
downstream tasks SKIPPED when something upstream fails. One event per attempt goes to data/control/pipeline_runs.jsonl.
This runs tasks one at a time; Airflow (Stage 14b) runs independent branches in parallel and adds a scheduler and UI.

  python -m src.ops.run_local daily --asof 2026-06-07 [--fast]
  python -m src.ops.run_local backfill --start 2026-06-01 --end 2026-06-07 [--fast]
--fast shortens the retry delays so a failure demo finishes in a minute."""
import argparse
import os
import subprocess
import sys
import time
from datetime import date, datetime, timezone

from src.ops import pipeline_spec as spec
from src.ops.task import emit, now_iso
from src.utils.config import ROOT


def run_pipeline(tasks, run_id, variables, fast=False):
    by = {t.name: t for t in tasks}
    result = {}
    for name in spec.order(tasks):
        t = by[name]
        blocked = [a for a in t.after if result.get(a, {}).get("status") != "SUCCESS"]
        if blocked:                                   # something upstream failed or was skipped: do not even start
            result[name] = {"status": "SKIPPED", "attempts": 0, "seconds": 0.0}
            emit({"run_id": run_id, "task": name, "attempt": 0, "status": "SKIPPED", "started_at": now_iso(),
                  "ended_at": now_iso(), "duration_s": 0, "exit_code": None,
                  "error_tail": "upstream not successful: " + ", ".join(blocked)})
            print(f"\n[{name}] SKIPPED (upstream not successful: {', '.join(blocked)})")
            continue

        cmd = [sys.executable, "-m", t.module, *spec.render_args(t, **variables)]
        t0, ok, attempts = time.time(), False, 0
        for attempt in range(1, t.retries + 2):
            attempts = attempt
            print(f"\n[{name}] attempt {attempt}/{t.retries + 1}: {' '.join(cmd[2:])}")
            rc = subprocess.call([sys.executable, "-m", "src.ops.task", name, "--", *cmd], cwd=ROOT,
                                 env={**os.environ, "PIPELINE_RUN_ID": run_id, "PIPELINE_ATTEMPT": str(attempt)})
            if rc == 0:
                ok = True
                break
            if attempt <= t.retries:
                # exponential backoff: 1x, 2x, 4x ... the first delay (capped at 10 minutes)
                wait = min((2 if fast else t.retry_delay_s) * 2 ** (attempt - 1), 600)
                print(f"[{name}] failed (exit {rc}); retrying in {wait}s")
                time.sleep(wait)
        result[name] = {"status": "SUCCESS" if ok else "FAILED", "attempts": attempts, "seconds": round(time.time() - t0, 1)}
        if not ok:
            print(f"\n[{name}] FAILED after {attempts} attempt(s)")
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pipeline", choices=["daily", "backfill"], nargs="?", default="daily")
    ap.add_argument("--asof", default=date.today().isoformat(), help="logical date of the run (YYYY-MM-DD)")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--fast", action="store_true")
    a = ap.parse_args()
    if a.pipeline == "backfill" and not (a.start and a.end):
        sys.exit("backfill needs --start and --end (YYYY-MM-DD): the booking-date window to recompute")

    tasks = spec.DAILY if a.pipeline == "daily" else spec.BACKFILL
    spec.validate(tasks)
    run_id = f"local_{a.pipeline}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    print(f"pipeline run {run_id}  asof={a.asof}")
    result = run_pipeline(tasks, run_id, {"asof": a.asof, "start": a.start or a.asof, "end": a.end or a.asof}, a.fast)

    print(f"\n=== {run_id} ===")
    print(f"{'task':<26}{'status':<10}{'attempts':>9}{'seconds':>9}")
    for name in spec.order(tasks):
        r = result[name]
        print(f"{name:<26}{r['status']:<10}{r['attempts']:>9}{r['seconds']:>9}")
    failed = [n for n, r in result.items() if r["status"] == "FAILED"]
    print("\nRESULT: " + ("ALL TASKS SUCCEEDED" if not failed and all(r["status"] == "SUCCESS" for r in result.values())
                          else "FAILED: " + ", ".join(failed)))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
