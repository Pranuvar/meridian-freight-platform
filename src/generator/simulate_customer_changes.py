"""Simulate customer reassignments in the TMS after the initial load.
Usage:  python -m src.generator.simulate_customer_changes 1     (later: 2)
Wave 1: 40 account-manager changes (8 also change region) on 2026-04-01, plus 3 renames (Type 1) on 2026-04-02.
Wave 2: 25 account-manager changes on 2026-05-01 (10 of them are customers already changed in wave 1)."""
import sys

import numpy as np
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://meridian:meridian_pw@localhost:5433/tms"
REGIONS = ["North", "South", "East", "West"]
WAVES = {
    1: dict(date="2026-04-01", rename_date="2026-04-02", n_mgr=40, n_region=8, n_rename=3, n_repeat=0, seed=101),
    2: dict(date="2026-05-01", rename_date=None, n_mgr=25, n_region=5, n_rename=0, n_repeat=10, seed=202),
}


def main(wave: int):
    cfg = WAVES[wave]
    rng = np.random.default_rng(cfg["seed"])
    eng = create_engine(DB_URL)
    with eng.begin() as c:
        rows = c.execute(text("SELECT customer_id, account_manager, region, CAST(updated_at AS date) "
                              "FROM tms.customers ORDER BY customer_id")).fetchall()
    cur = {r[0]: r for r in rows}
    if any(str(r[3]) == cfg["date"] for r in rows):
        print(f"wave {wave} already applied (customers stamped {cfg['date']}). Nothing to do.")
        return

    ids = sorted(cur)
    managers = sorted({r[1] for r in rows})
    wave1 = {i for i in ids if str(cur[i][3]) == WAVES[1]["date"]}
    if wave == 2 and not wave1:
        sys.exit("run wave 1 first")

    repeat = [int(i) for i in rng.choice(sorted(wave1), cfg["n_repeat"], replace=False)] if cfg["n_repeat"] else []
    pool = [i for i in ids if i not in wave1 and i not in set(repeat)]
    fresh = [int(i) for i in rng.choice(pool, cfg["n_mgr"] - len(repeat), replace=False)]
    mgr_ids = repeat + fresh
    region_ids = mgr_ids[:cfg["n_region"]]
    rename_pool = [i for i in pool if i not in set(fresh)]
    rename_ids = [int(i) for i in rng.choice(rename_pool, cfg["n_rename"], replace=False)] if cfg["n_rename"] else []

    ts = f"{cfg['date']} 09:00:00"
    with eng.begin() as c:
        for i in mgr_ids:
            new_mgr = str(rng.choice([m for m in managers if m != cur[i][1]]))
            c.execute(text("UPDATE tms.customers SET account_manager = :m, updated_at = :ts WHERE customer_id = :id"),
                      {"m": new_mgr, "ts": ts, "id": i})
        for i in region_ids:
            new_reg = str(rng.choice([r for r in REGIONS if r != cur[i][2]]))
            c.execute(text("UPDATE tms.customers SET region = :r, updated_at = :ts WHERE customer_id = :id"),
                      {"r": new_reg, "ts": ts, "id": i})
        for i in rename_ids:
            c.execute(text("UPDATE tms.customers SET customer_name = customer_name || ' Pvt Ltd', "
                           "updated_at = :ts WHERE customer_id = :id"),
                      {"ts": f"{cfg['rename_date']} 09:00:00", "id": i})
    print(f"wave {wave}: {len(mgr_ids)} manager changes ({len(repeat)} repeat customers), "
          f"{len(region_ids)} region changes, {len(rename_ids)} renames")


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("1", "2"):
        sys.exit("usage: python -m src.generator.simulate_customer_changes 1|2")
    main(int(sys.argv[1]))
