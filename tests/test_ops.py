import json
import os
import subprocess
import sys

import pytest

from src.ops import pipeline_spec as spec
from src.ops.health import freshness, volume_anomaly
from src.utils.config import ROOT


def test_pipelines_are_valid_and_run_upstream_first():
    for tasks in (spec.DAILY, spec.BACKFILL):
        spec.validate(tasks)
        pos = {n: i for i, n in enumerate(spec.order(tasks))}
        for t in tasks:
            assert all(pos[a] < pos[t.name] for a in t.after)


def test_publish_can_only_happen_after_a_clean_reconciliation():
    by = {t.name: t for t in spec.DAILY}
    assert by["publish"].after == ("reconcile",)          # the ONLY way to publish is through reconcile
    assert by["reconcile"].retries == 0                   # a FAIL is a finding: never retried away
    assert {"reconcile", "gold_facts", "gold_dim_customer", "silver_tables", "silver_reference", "bronze_files",
            "bronze_api", "check_postgres", "check_api", "check_landing"} <= spec.ancestors(spec.DAILY, "publish")


def test_every_task_points_at_a_module_that_exists():
    # Catches "the file was never written" before the pipeline hits it in the middle of a run.
    missing = [f"{t.name}: {spec.module_file(t)}" for t in spec.DAILY + spec.BACKFILL if not spec.module_file(t).exists()]
    assert not missing, missing


def test_a_cycle_is_rejected():
    a = spec.Task("a", "m", after=("b",))
    b = spec.Task("b", "m", after=("a",))
    with pytest.raises(ValueError, match="cycle"):
        spec.validate([a, b])


def _run_wrapper(tmp_path, code):
    events_file = tmp_path / "events.jsonl"
    env = {**os.environ, "PIPELINE_EVENTS_FILE": str(events_file), "PIPELINE_RUN_ID": "t1"}
    rc = subprocess.run([sys.executable, "-m", "src.ops.task", "demo", "--", sys.executable, "-c", code],
                        cwd=ROOT, env=env, capture_output=True, text=True).returncode
    return rc, [json.loads(line) for line in events_file.read_text().splitlines()]


def test_wrapper_records_a_successful_task(tmp_path):
    rc, events = _run_wrapper(tmp_path, "print('hello')")
    assert rc == 0 and len(events) == 1
    assert events[0]["status"] == "SUCCESS" and events[0]["run_id"] == "t1" and events[0]["error_tail"] == ""


def test_wrapper_records_a_failure_and_passes_the_exit_code_on(tmp_path):
    rc, events = _run_wrapper(tmp_path, "import sys; print('boom line'); sys.exit(3)")
    assert rc == 3                                         # the orchestrator must see the failure
    assert events[0]["status"] == "FAILED" and events[0]["exit_code"] == 3 and "boom line" in events[0]["error_tail"]


def test_freshness_limit():
    assert freshness(10, 36) == "OK" and freshness(40, 36) == "STALE"


def test_volume_anomaly_needs_history_and_uses_the_median():
    assert volume_anomaly([100, 110], 100)[0] == "INSUFFICIENT_HISTORY"
    assert volume_anomaly([100, 110, 90, 105], 103)[0] == "OK"
    assert volume_anomaly([100, 110, 90, 105], 30)[0] == "ANOMALY"      # far too few rows
    assert volume_anomaly([100, 110, 90, 105], 500)[0] == "ANOMALY"     # far too many
