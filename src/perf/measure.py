"""Measurement helpers for the performance lab.
Wall-clock time alone is noisy, so every run also records what SPARK reports for the stages it ran: tasks, bytes
read from files (input), and bytes moved between stages (shuffle). Those come from the Spark UI's REST API, so they
are the same numbers you see in the UI at http://localhost:4040."""
import os
import re
import sys
import time
from contextlib import contextmanager, redirect_stdout
from io import StringIO

import requests


def start_spark(name):
    # Python UDF workers are separate processes that Spark launches using PYSPARK_PYTHON. Point them at THIS
    # interpreter (the venv), or they could start a different Python that has no pandas or pyarrow.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from src.utils.spark import get_spark
    return get_spark(name)


@contextmanager
def conf(spark, settings):
    """Temporarily change Spark SQL settings, then put them back exactly as they were."""
    old = {k: spark.conf.get(k, None) for k in settings}
    for k, v in settings.items():
        spark.conf.set(k, v)
    try:
        yield
    finally:
        for k, v in old.items():
            spark.conf.unset(k) if v is None else spark.conf.set(k, v)


def noop(df):
    """Execute the WHOLE plan and throw the result away (Spark's 'noop' sink). This times the computation itself,
    without the cost of writing output files."""
    df.write.format("noop").mode("overwrite").save()


def _ui_stages(spark):
    """Completed stages from the Spark UI REST API as {(stageId, attemptId): stage}, or None if unreachable."""
    sc = spark.sparkContext
    for base in (sc.uiWebUrl, "http://localhost:4040"):
        try:
            r = requests.get(f"{base}/api/v1/applications/{sc.applicationId}/stages",
                             params={"status": "complete"}, timeout=5)
            r.raise_for_status()
            return {(s["stageId"], s["attemptId"]): s for s in r.json()}
        except Exception:
            continue
    return None


def _delta(before, after):
    """Metrics of the stages that finished BETWEEN two snapshots."""
    if before is None or after is None:
        return {}
    new = [after[k] for k in after.keys() - before.keys()]
    mb = lambda field: round(sum(s[field] for s in new) / 1e6, 1)       # noqa: E731
    return {"stages": len(new), "tasks": sum(s["numCompleteTasks"] for s in new), "input_mb": mb("inputBytes"),
            "shuf_read_mb": mb("shuffleReadBytes"), "shuf_write_mb": mb("shuffleWriteBytes")}


def run(spark, label, fn, repeats=2):
    """Run fn() `repeats` times and keep the FASTEST run (the minimum is the standard way to cut timing noise).
    Returns that run's seconds, its Spark stage metrics, and whatever fn returned (used for correctness checks)."""
    best = None
    for _ in range(repeats):
        before = _ui_stages(spark)
        t0 = time.perf_counter()
        out = fn()
        secs = time.perf_counter() - t0
        time.sleep(1.0)                      # the UI learns about finished stages asynchronously
        stats = _delta(before, _ui_stages(spark))
        if best is None or secs < best["seconds"]:
            best = {"label": label, "seconds": secs, "result": out, **stats}
    return best


def report(title, rows):
    print(f"\n=== {title} ===")
    print(f"{'variant':<56}{'seconds':>9}{'stages':>8}{'tasks':>8}{'input MB':>10}{'shuf.rd MB':>12}{'shuf.wr MB':>12}")
    for r in rows:
        cell = lambda k: f"{r[k]:,}" if k in r else "n/a"               # noqa: E731
        print(f"{r['label']:<56}{r['seconds']:>9.1f}{cell('stages'):>8}{cell('tasks'):>8}{cell('input_mb'):>10}"
              f"{cell('shuf_read_mb'):>12}{cell('shuf_write_mb'):>12}")
    fastest, slowest = min(r["seconds"] for r in rows), max(r["seconds"] for r in rows)
    print(f"slowest / fastest = {slowest / fastest:.1f}x   (fastest of the repeated runs; your absolute times will differ)")


def _explain_text(df):
    buf = StringIO()
    with redirect_stdout(buf):       # df.explain() prints the physical plan; capture it instead of showing it
        df.explain()
    return buf.getvalue()


def show_plan(label, df, *operators):
    """Count the operators we care about in the physical plan. Use with adaptive execution OFF, otherwise the plan
    Spark prints up front can differ from the one it finally runs."""
    text = _explain_text(df)
    found = ", ".join(f"{p.replace(chr(92), '')} x{len(re.findall(p, text))}" for p in operators if re.search(p, text))
    print(f"  plan [{label}]: {found or 'none of the listed operators'}")


def scan_filters(df):
    """The PartitionFilters / PushedFilters Spark hands to the file scan: proof of partition pruning and pushdown."""
    return re.findall(r"(?:PartitionFilters|PushedFilters): \[[^\]]*\]", _explain_text(df))
