"""The pipeline as DATA: tasks, commands and dependencies in ONE place.
The local runner, the Airflow DAG (Stage 14b) and the tests all read this file, so the three can never disagree.
This module must stay plain Python (no pyspark), because Airflow imports it in its own environment."""
from dataclasses import dataclass

from src.utils.config import ROOT


@dataclass(frozen=True)
class Task:
    name: str
    module: str                # run as:  python -m <module> <args>
    args: tuple = ()           # may contain {asof}, {start}, {end} placeholders
    after: tuple = ()          # names of the tasks that must SUCCEED first
    retries: int = 2           # extra attempts. Safe ONLY because every job is idempotent.
    retry_delay_s: int = 60    # first delay. Doubles on every further attempt (exponential backoff).
    description: str = ""


DAILY = [
    # ---- are the sources there? (cheap checks first: fail fast before any Spark starts)
    Task("check_postgres", "src.ops.checks", ("postgres",), (), 3, 30, "TMS database reachable"),
    Task("check_api", "src.ops.checks", ("api",), (), 5, 30, "reference API healthy (it is deliberately flaky)"),
    Task("check_landing", "src.ops.checks", ("landing",), (), 0, 0, "scan and invoice folders present and non-empty"),
    # ---- extract + Bronze (append-only)
    Task("bronze_api", "src.bronze.ingest_api_incremental", ("{asof}",), ("check_api",), 2, 60, "FX + fuel, 7-day lookback"),
    Task("bronze_files", "src.bronze.ingest_incremental", (), ("check_postgres", "check_landing"), 2, 60,
         "Postgres watermark + new scan/invoice files"),
    # ---- Silver (quality rules and quarantine happen inside these jobs)
    Task("silver_tables", "src.silver.incremental", (), ("bronze_files",), 2, 60, "validate, dedupe, quarantine, MERGE"),
    Task("silver_reference", "src.silver.build_reference", (), ("bronze_api",), 2, 60, "typed FX + fuel"),
    # ---- Gold
    Task("gold_dim_customer", "src.gold.build_dim_customer", (), ("silver_tables",), 2, 60, "SCD Type 2 merge"),
    Task("gold_facts", "src.gold.incremental", (), ("gold_dim_customer", "silver_reference"), 2, 60, "affected shipments only"),
    # ---- the gate. retries=0: a reconciliation FAIL is a FINDING, not a transient error.
    Task("reconcile", "src.reconciliation.run", (), ("gold_facts",), 0, 0, "exit code 1 when any check FAILS"),
    Task("publish", "src.ops.publish", (), ("reconcile",), 0, 0, "record the Gold versions consumers may read"),
    Task("serve_export", "src.serve.export", (), ("publish",), 1, 60, "published Gold versions -> Parquet for Power BI"),
]

BACKFILL = [
    Task("backfill_facts", "src.gold.incremental", ("--backfill", "{start}", "{end}"), (), 1, 60,
         "recompute shipments BOOKED in the window; watermarks untouched"),
    Task("reconcile_after_backfill", "src.reconciliation.run", (), ("backfill_facts",), 0, 0, "prove the books still balance"),
]


def validate(tasks):
    names = [t.name for t in tasks]
    if len(set(names)) != len(names):
        raise ValueError("duplicate task names")
    for t in tasks:
        for a in t.after:
            if a not in names:
                raise ValueError(f"{t.name}: unknown upstream task {a!r}")
    order(tasks)                      # raises if there is a cycle


def order(tasks):
    """A valid execution order (Kahn's algorithm). Repeatedly take every task whose upstreams are all done.
    If none is ready but tasks remain, they wait on each other in a cycle."""
    remaining = {t.name: set(t.after) for t in tasks}
    out = []
    while remaining:
        ready = [n for n, deps in remaining.items() if not deps]       # dicts keep declaration order: stable output
        if not ready:
            raise ValueError("cycle among: " + ", ".join(remaining))
        for n in ready:
            out.append(n)
            del remaining[n]
        for deps in remaining.values():
            deps.difference_update(ready)
    return out


def ancestors(tasks, name):
    """Every task that must run (directly or indirectly) before `name`."""
    by = {t.name: t for t in tasks}
    seen, stack = set(), list(by[name].after)
    while stack:
        n = stack.pop()
        if n not in seen:
            seen.add(n)
            stack.extend(by[n].after)
    return seen


def render_args(task, **variables):
    return [a.format(**variables) for a in task.args]


def module_file(task):
    return ROOT / (task.module.replace(".", "/") + ".py")
