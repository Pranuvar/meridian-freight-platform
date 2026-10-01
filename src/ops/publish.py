"""Publish marker: records WHICH VERSION of every Gold table is now safe for reporting.
It only runs after reconciliation succeeds (the DAG enforces that). A consumer reads these versions with Delta time
travel, so a half-finished or failed run can never leak into a report."""
import json
import os
from datetime import datetime, timezone

from src.utils.config import CONTROL, GOLD


def delta_version(table_dir):
    """Latest commit number of a Delta table = highest NNN in _delta_log/NNN.json."""
    logs = list((table_dir / "_delta_log").glob("*.json"))
    return max(int(p.stem) for p in logs) if logs else None


def main():
    tables = {p.name: delta_version(p) for p in sorted(GOLD.iterdir()) if (p / "_delta_log").exists()}
    marker = {"published_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "run_id": os.getenv("PIPELINE_RUN_ID") or os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or "manual",
              "gold_table_versions": tables}
    CONTROL.mkdir(parents=True, exist_ok=True)
    tmp, final = CONTROL / "published.json.tmp", CONTROL / "published.json"
    tmp.write_text(json.dumps(marker, indent=2))
    os.replace(tmp, final)                      # atomic: a reader sees the old file or the new one, never half a file
    print(f"published {len(tables)} Gold tables at {marker['published_at']}")
    for name, v in tables.items():
        print(f"  {name:<28} version {v}")


if __name__ == "__main__":
    main()
