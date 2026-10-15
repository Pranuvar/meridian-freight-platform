"""Export the CONTROL data that reconciliation needs, as one ZIP for the Databricks Volume.

Needs the Postgres container running:  docker compose up -d
Run from the project root:   python -m src.export.control_export
"""
import json
import zipfile
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://meridian:meridian_pw@localhost:5433/tms"
DATA = Path("data")
OUT = DATA / "landing_export"
STAGE = OUT / "_control"


def source_totals(engine):
    with engine.connect() as c:
        entities = {t: c.execute(text(f"SELECT COUNT(*) FROM tms.{t}")).scalar()
                    for t in ["customers", "shipments", "shipment_charges", "claims"]}
        rows = c.execute(text("SELECT currency, COUNT(*) AS n, SUM(amount)::numeric(20,2) AS s "
                              "FROM tms.shipment_charges GROUP BY currency")).fetchall()
    # sums travel as TEXT so no float ever touches a money total
    return {"entities": entities, "charges_by_currency": {r[0]: {"n": int(r[1]), "s": str(r[2])} for r in rows}}


def ledger():
    parts = [pd.read_parquet(DATA / "truth" / f, columns=["shipment_ref", "billed_inr"])
             for f in ("shipment_truth.parquet", "shipment_truth_inc1.parquet") if (DATA / "truth" / f).exists()]
    return pd.concat(parts, ignore_index=True)


def held_delivered():
    held = pd.read_parquet(DATA / "held_back" / "scans_held_after_inc1.parquet", columns=["shipment_ref", "event_type"])
    held = held[held.event_type == "DELIVERED"]
    # same normalisation Silver applies: MFL20250000123 -> MFL-2025-0000123
    refs = held.shipment_ref.str.replace(r"^MFL-?(\d{4})-?(\d{7})$", r"MFL-\1-\2", regex=True)
    return pd.DataFrame({"shipment_ref": refs.drop_duplicates()})


def main():
    STAGE.mkdir(parents=True, exist_ok=True)
    (STAGE / "source_totals.json").write_text(json.dumps(source_totals(create_engine(DB_URL)), indent=2))
    ledger().to_parquet(STAGE / "ledger_billed.parquet", index=False)
    held_delivered().to_parquet(STAGE / "held_delivered_refs.parquet", index=False)
    target = OUT / "meridian_control.zip"
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(STAGE.iterdir()):
            z.write(f, f"control/{f.name}")
    print(f"{target}  ({target.stat().st_size / 1e6:.1f} MB)")
    print(json.dumps(json.loads((STAGE / "source_totals.json").read_text()), indent=2))


if __name__ == "__main__":
    main()
