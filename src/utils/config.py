"""Central place for paths and connection settings, so no script hardcodes them."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]   # .../meridian-freight-platform
DATA = ROOT / "data"
RAW = DATA / "raw"
HELD = DATA / "held_back"
BRONZE, SILVER, GOLD = DATA / "bronze", DATA / "silver", DATA / "gold"
QUARANTINE = DATA / "quarantine"
CONTROL = DATA / "control"          # watermarks and run logs (later stages)

# Postgres from the host. Port 5433 is the mapping in docker-compose.yml.
PG_URL = "jdbc:postgresql://localhost:5433/tms"
PG_PROPS = {"user": "meridian", "password": "meridian_pw", "driver": "org.postgresql.Driver"}


def bronze_path(name: str) -> str:
    return str(BRONZE / name)
