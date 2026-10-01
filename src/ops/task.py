"""Run ONE pipeline command and record the outcome.   python -m src.ops.task NAME -- <command...>
Streams the command's output (so the orchestrator's log shows it), keeps the last lines for the error report,
appends one JSON line per attempt to the run-history file, and exits with the command's own exit code."""
import json
import os
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from src.utils.config import CONTROL

# Overridable so tests never write into the real history.
EVENTS = Path(os.getenv("PIPELINE_EVENTS_FILE", str(CONTROL / "pipeline_runs.jsonl")))


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def emit(event: dict):
    EVENTS.parent.mkdir(parents=True, exist_ok=True)
    with open(EVENTS, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")      # ONE write call per event: a short line append is effectively atomic


def main():
    if len(sys.argv) < 4 or "--" not in sys.argv:
        sys.exit("usage: python -m src.ops.task NAME -- <command...>")
    name = sys.argv[1]
    cmd = sys.argv[sys.argv.index("--") + 1:]
    run_id = (os.getenv("PIPELINE_RUN_ID") or os.getenv("AIRFLOW_CTX_DAG_RUN_ID")
              or f"manual_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}")
    attempt = int(os.getenv("PIPELINE_ATTEMPT") or os.getenv("AIRFLOW_CTX_TRY_NUMBER") or 1)

    started, t0 = now_iso(), time.time()
    tail = deque(maxlen=25)                                   # only the last 25 lines: enough to see WHY it failed
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        tail.append(line.rstrip()[:300])
    rc = proc.wait()

    emit({"run_id": run_id, "task": name, "attempt": attempt, "status": "SUCCESS" if rc == 0 else "FAILED",
          "started_at": started, "ended_at": now_iso(), "duration_s": round(time.time() - t0, 1),
          "exit_code": rc, "error_tail": "" if rc == 0 else "\n".join(tail)})
    sys.exit(rc)


if __name__ == "__main__":
    main()
