"""Airflow DAGs GENERATED from src/ops/pipeline_spec.py.
The tasks, dependencies and retry settings are not written here: they come from the same spec the local runner and the
tests use, so they cannot drift apart. Every task runs the pipeline's own interpreter (PY), NOT Airflow's, through
src.ops.task, which records the attempt in data/control/pipeline_runs.jsonl."""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT))                     # so Airflow's environment can import src.ops.pipeline_spec
from src.ops import pipeline_spec as spec            # noqa: E402  (plain Python: no pyspark needed here)

try:                                                 # Airflow 3
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:                                  # Airflow 2 fallback
    from airflow import DAG
    from airflow.operators.bash import BashOperator

PY = str(PROJECT / ".venv" / "bin" / "python")       # the PIPELINE's environment
FAST = bool(os.getenv("MERIDIAN_FAST"))              # failure drills: 2-second retry delays instead of minutes

# The spec uses {asof} style placeholders; Airflow fills Jinja templates from the run's parameters instead.
JINJA = {"{asof}": "{{ params.asof }}", "{start}": "{{ params.start }}", "{end}": "{{ params.end }}"}


def jinja(arg):
    for old, new in JINJA.items():
        arg = arg.replace(old, new)
    return arg


def build(dag_id, tasks, schedule, params, description):
    spec.validate(tasks)                             # a cycle or a typo fails at import time, not at 6am
    with DAG(dag_id=dag_id, description=description, schedule=schedule, start_date=datetime(2026, 6, 1),
             catchup=False,                          # never auto-run every missed interval
             max_active_runs=1,                      # Silver and Gold must never run at the same time
             params=params, tags=["meridian"]) as dag:
        ops = {}
        for t in tasks:
            args = " ".join(jinja(a) for a in t.args)
            ops[t.name] = BashOperator(
                task_id=t.name,
                bash_command=f"cd {PROJECT} && {PY} -m src.ops.task {t.name} -- {PY} -m {t.module} {args}",
                env={"PIPELINE_RUN_ID": "{{ run_id }}", "PIPELINE_ATTEMPT": "{{ ti.try_number }}"},
                append_env=True,                     # keep PATH etc.; without it the env above REPLACES the environment
                retries=t.retries,
                retry_delay=timedelta(seconds=2 if FAST else max(t.retry_delay_s, 1)),
                retry_exponential_backoff=True,      # 1x, 2x, 4x ... the first delay
                max_retry_delay=timedelta(minutes=10),
                execution_timeout=timedelta(minutes=45),   # a hung task must fail, not block the pipeline forever
            )
        for t in tasks:
            for upstream in t.after:
                ops[upstream] >> ops[t.name]         # the dependency arrows
    return dag


# The simulated business ends on 7 June, so that is the default. Production would pass the run's own date.
daily = build("meridian_daily", spec.DAILY, "0 6 * * *", {"asof": "2026-06-07"},
              "Extract, Bronze, Silver, Gold, reconcile, publish")
backfill = build("meridian_backfill", spec.BACKFILL, None, {"start": "2026-06-01", "end": "2026-06-07"},
                 "Recompute Gold for shipments booked in a window, then reconcile")
