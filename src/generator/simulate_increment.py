"""Simulate ONE WEEK of business after the initial load (2026-06-01 .. 2026-06-07).

  Postgres  : in-flight shipments progress (UPDATE), 2,700 new shipments + charges (INSERT),
              20 customers get a new account manager on 2026-06-03 (input for SCD2)
  Scan files: daily files = the scans we HELD BACK at the initial load (late arrivals and events of
              in-flight shipments) + scans of the new shipments
  Invoices  : June invoice lines for delivered shipments that were not yet invoiced at the cutoff
  Truth     : the correct answers, written to data/truth for verification in later stages

Usage:  python -m src.generator.simulate_increment
"""
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

import src.generator.generate as gen

WIN_START = pd.Timestamp("2026-06-01")
WIN_END = pd.Timestamp("2026-06-07 23:59:59")
N_NEW = 2700
N_CUSTOMER_CHANGES = 20
TMP = Path("data/tmp_inc")
SCAN_COLS = ["shipment_ref", "event_type", "event_ts", "location_code", "scan_source", "arrival_date"]
TRUTH_COLS = ["shipment_id", "shipment_ref", "customer_id", "carrier_id", "service_level", "status",
              "promised_true", "pickup_at", "delivered_at", "quoted_true"]


def main():
    raw, truth_dir, held = gen.RAW, gen.TRUTH, gen.HELD      # real locations, captured BEFORE we patch gen
    scans_dir = raw / "scans"
    eng = create_engine(gen.DB_URL)

    with eng.connect() as c:
        already = c.execute(text("SELECT COUNT(*) FROM tms.shipments WHERE booked_at >= :d"),
                            {"d": str(WIN_START)}).scalar()
    if already or list(scans_dir.glob("scans_202606*.parquet")):
        sys.exit("increment already applied: June rows or files exist.")

    rng = np.random.default_rng(303)
    truth0 = pd.read_parquet(truth_dir / "shipment_truth.parquet")
    locations = gen.build_locations()

    # ---- 1) in-flight shipments progress. The truth file knows their real pickup/delivery times.
    inflight = truth0[truth0.status.isin(["BOOKED", "IN_TRANSIT"])]
    deliver = inflight[inflight.delivered_at <= WIN_END]
    transit = inflight[(inflight.status == "BOOKED") & (inflight.delivered_at > WIN_END)
                       & (inflight.pickup_at <= WIN_END)]
    status_rows = (
        [{"id": int(r.shipment_id), "st": "DELIVERED", "ts": r.delivered_at.strftime("%Y-%m-%d %H:%M:%S")}
         for r in deliver.itertuples()]
        + [{"id": int(r.shipment_id), "st": "IN_TRANSIT", "ts": r.pickup_at.strftime("%Y-%m-%d %H:%M:%S")}
           for r in transit.itertuples()])

    # ---- 2) brand-new shipments: reuse the original generator, pointed at the new week.
    customers = pd.read_sql("SELECT customer_id FROM tms.customers", eng)
    hist = pd.read_sql("SELECT customer_id FROM tms.shipments", eng).customer_id.values
    max_id = int(pd.read_sql("SELECT MAX(shipment_id) AS m FROM tms.shipments", eng)["m"][0])
    gen.N_SHIPMENTS, gen.START, gen.CUTOFF, gen.rng = N_NEW, WIN_START, WIN_END, rng
    ship = gen.build_shipments(customers, locations)
    ship["shipment_id"] = max_id + np.arange(1, N_NEW + 1)                                   # continue the id sequence
    ship["shipment_ref"] = "MFL-2026-" + ship.shipment_id.astype(str).str.zfill(7)
    # Same customer skew as history: resample customers from the existing shipments.
    ship["customer_id"] = rng.choice(hist, N_NEW)
    charges = gen.build_charges(ship).drop(columns=["charge_id"])   # Postgres assigns charge_id itself

    # ---- 3) customer reassignments on 2026-06-03
    cur = pd.read_sql("SELECT customer_id, account_manager FROM tms.customers", eng)
    managers = sorted(cur.account_manager.unique())
    current = dict(zip(cur.customer_id, cur.account_manager))
    picks = rng.choice(cur.customer_id.values, N_CUSTOMER_CHANGES, replace=False)
    cust_rows = [{"id": int(i), "m": str(rng.choice([m for m in managers if m != current[i]])),
                  "ts": "2026-06-03 09:00:00"} for i in picks]

    # ---- 4) scans. The ORIGINAL write_scans builds events (with all its mess) for the new shipments,
    #         writing into a temp folder. We then merge them with the held-back rows, per arrival day.
    shutil.rmtree(TMP, ignore_errors=True)
    gen.RAW, gen.HELD = TMP / "raw", TMP / "held"
    gen.write_scans(ship, locations)
    new_past = pd.concat(
        [pd.read_parquet(f).assign(arrival_date=pd.to_datetime(f.stem.split("_")[1], format="%Y%m%d"))
         for f in sorted((TMP / "raw" / "scans").glob("scans_*.parquet"))], ignore_index=True)
    new_future = pd.read_parquet(TMP / "held" / "scans_future.parquet")
    orig = pd.read_parquet(held / "scans_future.parquet")
    end_day = WIN_END.normalize()
    released = pd.concat([orig[orig.arrival_date <= end_day][SCAN_COLS], new_past[SCAN_COLS]], ignore_index=True)
    still_held = pd.concat([orig[orig.arrival_date > end_day][SCAN_COLS], new_future[SCAN_COLS]], ignore_index=True)

    # ---- 5) invoices: a catch-up for delivered shipments that had NO invoice at the cutoff
    cand = truth0[(truth0.status == "DELIVERED") & truth0.carrier_invoiced_true.isna()]
    n = len(cand)
    over = rng.random(n) < 0.06                                     # 6% overcharged by 10-25%
    total = cand.quoted_true.values * np.where(over, rng.uniform(1.10, 1.25, n), rng.normal(1.0, 0.015, n))
    days = (WIN_END.normalize() - WIN_START).days + 1
    base = pd.DataFrame({
        "shipment_id": cand.shipment_id.values, "shipment_ref": cand.shipment_ref.values,
        "carrier_id": cand.carrier_id.values,
        "inv_date": WIN_START + pd.to_timedelta(rng.integers(0, days, n), unit="D")})
    parts = []
    for ltype, share in [("FREIGHT", 0.88), ("FUEL", 0.12)]:
        x = base.copy()
        x["line_type"], x["amount"] = ltype, (total * share).round(2)
        parts.append(x)
    det = rng.random(n) < 0.08                                      # 8% get a detention charge
    x = base[det].copy()
    x["line_type"], x["amount"] = "DETENTION", rng.uniform(500, 2500, int(det.sum())).round(2)
    parts.append(x)
    L = pd.concat(parts, ignore_index=True)
    inv_true = L.groupby("shipment_id").amount.sum()                # the true carrier cost, before any mess
    dups = L.sample(frac=0.03, random_state=11)                     # duplicate lines
    ghost = L.sample(frac=0.005, random_state=12).copy()            # lines for shipments that don't exist
    ghost["shipment_ref"] = ["MFL-2026-" + str(v) for v in rng.integers(9_000_000, 9_999_999, len(ghost))]
    L2 = pd.concat([L, dups, ghost], ignore_index=True).sort_values("inv_date").reset_index(drop=True)

    # ---- apply everything to Postgres in ONE transaction: all or nothing
    with eng.begin() as c:
        if status_rows:
            c.execute(text("UPDATE tms.shipments SET status = :st, updated_at = :ts WHERE shipment_id = :id"),
                      status_rows)
        ship[gen.SHIP_COLS].to_sql("shipments", c, schema="tms", if_exists="append", index=False,
                                   method="multi", chunksize=5000)
        charges.to_sql("shipment_charges", c, schema="tms", if_exists="append", index=False,
                       method="multi", chunksize=5000)
        c.execute(text("UPDATE tms.customers SET account_manager = :m, updated_at = :ts WHERE customer_id = :id"),
                  cust_rows)
        c.execute(text("SELECT setval(pg_get_serial_sequence('tms.shipments','shipment_id'), "
                       "(SELECT MAX(shipment_id) FROM tms.shipments))"))

    # ---- files (only after the database commit succeeded)
    scans_dir.mkdir(parents=True, exist_ok=True)
    for d, g in released.groupby("arrival_date"):
        g.drop(columns=["arrival_date"]).to_parquet(scans_dir / f"scans_{d:%Y%m%d}.parquet", index=False)
    still_held.to_parquet(held / "scans_held_after_inc1.parquet", index=False)

    inv_files = 0
    for cid, (code, fn, kw) in gen.LAYOUTS.items():
        sub = L2[L2.carrier_id == cid]
        folder = raw / "invoices" / code
        for month, g in sub.groupby(sub.inv_date.dt.strftime("%Y-%m")):
            fn(g).to_csv(folder / f"invoices_{code}_{month}.csv", index=False, **kw)
            inv_files += 1

    # ---- truth for this increment
    inv_true.rename("carrier_invoiced_true_inc1").reset_index().to_parquet(
        truth_dir / "invoice_truth_inc1.parquet", index=False)
    billed = (charges.amount * np.where(charges.currency == "USD", gen.USD_INR, 1.0)).groupby(charges.shipment_id).sum()
    t = ship.loc[ship.status != "CANCELLED", TRUTH_COLS].copy()
    t["on_time"] = t.delivered_at <= t.promised_true
    t["billed_inr"] = t.shipment_id.map(billed)
    t["carrier_invoiced_true"] = np.nan
    t.to_parquet(truth_dir / "shipment_truth_inc1.parquet", index=False)
    shutil.rmtree(TMP, ignore_errors=True)

    print(f"status changes      : {len(deliver):,} -> DELIVERED, {len(transit):,} -> IN_TRANSIT")
    print(f"new shipments       : {N_NEW:,} (+{len(charges):,} charge lines)")
    print(f"customer changes    : {len(cust_rows)} on 2026-06-03")
    print(f"scan files released : {released.arrival_date.nunique()} days, {len(released):,} rows "
          f"({len(released) - len(new_past):,} held-back + {len(new_past):,} from new shipments); "
          f"still held back: {len(still_held):,}")
    print(f"invoices            : {len(L2):,} lines in {inv_files} files ({n:,} previously un-invoiced shipments)")


if __name__ == "__main__":
    main()
