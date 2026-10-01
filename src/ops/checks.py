"""Source availability checks, run BEFORE any Spark job.  python -m src.ops.checks postgres|api|landing
A failed check exits non-zero with a clear message, so the orchestrator retries it or stops the run early."""
import sys
from datetime import datetime

import requests

from src.utils.config import RAW

# Same credentials as docker-compose.yml. In a real deployment these come from a secret store, never the code.
PG_SQLALCHEMY_URL = "postgresql+psycopg2://meridian:meridian_pw@localhost:5433/tms"
API_HEALTH_URL = "http://localhost:8000/health"


def postgres():
    from sqlalchemy import create_engine, text
    eng = create_engine(PG_SQLALCHEMY_URL, connect_args={"connect_timeout": 5})
    with eng.connect() as c:
        newest = c.execute(text("SELECT MAX(updated_at) FROM tms.shipments")).scalar()
    print(f"postgres reachable. newest shipment update: {newest}")


def api():
    r = requests.get(API_HEALTH_URL, timeout=5)
    r.raise_for_status()
    if r.json().get("status") != "ok":
        raise RuntimeError(f"unexpected health response: {r.text[:100]}")
    print("reference API healthy")


def landing():
    folders = [RAW / "scans"] + [RAW / "invoices" / c for c in ("BLZ", "SWF", "TRK")]
    for f in folders:
        files = [p for p in f.glob("*") if p.is_file()] if f.exists() else []
        if not files:
            raise RuntimeError(f"{f} is missing or empty")
        newest = max(files, key=lambda p: p.stat().st_mtime)
        print(f"{f.relative_to(RAW.parent.parent)}: {len(files)} files, newest {newest.name} "
              f"({datetime.fromtimestamp(newest.stat().st_mtime):%Y-%m-%d %H:%M})")


CHECKS = {"postgres": postgres, "api": api, "landing": landing}


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else ""
    if which not in CHECKS:
        sys.exit(f"usage: python -m src.ops.checks {'|'.join(CHECKS)}")
    try:
        CHECKS[which]()
    except Exception as e:                      # any failure -> non-zero exit + one readable line
        sys.exit(f"{which} check FAILED: {type(e).__name__}: {str(e)[:250]}")


if __name__ == "__main__":
    main()
