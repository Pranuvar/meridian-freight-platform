"""
Synthetic data generator for Meridian Freight.

ONE consistent "ground truth" is generated first, then split into four messy sources:
  1. Postgres TMS tables          -> operational DB (shipments, customers, charges, claims)
  2. Daily Parquet scan files     -> tracking events, one file per ARRIVAL day (not event day)
  3. Carrier invoice CSVs         -> 3 carriers, 3 different layouts, monthly files
  4. data/truth/shipment_truth.parquet -> the real answers. The pipeline never reads this.
     It exists so that in Stage 9+ we can PROVE our on-time %, margin etc. are correct.

Cutoff: everything up to 2026-05-31 is the INITIAL load. Later scans are held back
(data/held_back) and become the INCREMENT we process in Stage 8.

Run from the project root:   python -m src.generator.generate
"""
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text

SEED = 42
rng = np.random.default_rng(SEED)   # one seeded generator => same data every run
Faker.seed(SEED)
fake = Faker("en_IN")

DB_URL = "postgresql+psycopg2://meridian:meridian_pw@localhost:5433/tms"
RAW, TRUTH, HELD = Path("data/raw"), Path("data/truth"), Path("data/held_back")

N_CUSTOMERS = 800
N_SHIPMENTS = 200_000
START = pd.Timestamp("2025-01-01")
CUTOFF = pd.Timestamp("2026-05-31 23:59:59")
USD_INR = 84.0


def td(values, unit):
    """Array of numbers -> numpy timedelta64 array. Adding a plain numpy array to a Series is
    POSITIONAL, so it can never misalign on a non-default index."""
    return pd.to_timedelta(values, unit=unit).to_numpy()


# ------------------------------------------------------------------ reference data
LOCATIONS = [  # code, name, city, state, region, type
    ("DEL01", "Delhi Hub", "Delhi", "Delhi", "North", "HUB"),
    ("BOM01", "Mumbai Hub", "Mumbai", "Maharashtra", "West", "HUB"),
    ("BLR01", "Bengaluru Hub", "Bengaluru", "Karnataka", "South", "HUB"),
    ("MAA01", "Chennai Hub", "Chennai", "Tamil Nadu", "South", "HUB"),
    ("HYD01", "Hyderabad Hub", "Hyderabad", "Telangana", "South", "HUB"),
    ("CCU01", "Kolkata Hub", "Kolkata", "West Bengal", "East", "HUB"),
    ("JAI02", "Jaipur Depot", "Jaipur", "Rajasthan", "North", "CUSTOMER_SITE"),
    ("LKO02", "Lucknow Depot", "Lucknow", "Uttar Pradesh", "North", "CUSTOMER_SITE"),
    ("AMD02", "Ahmedabad Depot", "Ahmedabad", "Gujarat", "West", "CUSTOMER_SITE"),
    ("PNQ02", "Pune Depot", "Pune", "Maharashtra", "West", "CUSTOMER_SITE"),
    ("NAG02", "Nagpur Depot", "Nagpur", "Maharashtra", "West", "CUSTOMER_SITE"),
    ("COK02", "Kochi Depot", "Kochi", "Kerala", "South", "CUSTOMER_SITE"),
    ("VTZ02", "Visakhapatnam Depot", "Visakhapatnam", "Andhra Pradesh", "South", "CUSTOMER_SITE"),
    ("CJB02", "Coimbatore Depot", "Coimbatore", "Tamil Nadu", "South", "CUSTOMER_SITE"),
    ("BBI02", "Bhubaneswar Depot", "Bhubaneswar", "Odisha", "East", "CUSTOMER_SITE"),
    ("PAT02", "Patna Depot", "Patna", "Bihar", "East", "CUSTOMER_SITE"),
    ("NSA01", "Nhava Sheva Port", "Navi Mumbai", "Maharashtra", "West", "PORT"),
    ("MUN01", "Mundra Port", "Mundra", "Gujarat", "West", "PORT"),
]
# code, name, probability a delivery is LATE. TruckNet is deliberately the worst carrier so the
# BI layer has a real story to find ("which carrier is hurting on-time delivery?").
CARRIERS = [("BLZ", "BlueLine Logistics", 0.12), ("SWF", "SwiftHaul Carriers", 0.18),
            ("TRK", "TruckNet India", 0.30)]
LATE_PROB = np.array([c[2] for c in CARRIERS])


def build_locations():
    df = pd.DataFrame(LOCATIONS, columns=["location_code", "name", "city", "state", "region", "location_type"])
    df.insert(0, "location_id", np.arange(1, len(df) + 1))
    return df


def build_carriers():
    df = pd.DataFrame([(c[0], c[1]) for c in CARRIERS], columns=["carrier_code", "carrier_name"])
    df.insert(0, "carrier_id", np.arange(1, len(CARRIERS) + 1))
    df["mode"], df["is_active"], df["updated_at"] = "ROAD", True, START
    return df


def build_customers():
    n = N_CUSTOMERS
    seg = pd.Series(rng.choice(["Enterprise", "Mid-Market", "SME"], n, p=[0.10, 0.30, 0.60]))
    dirty = rng.random(n) < 0.03                    # DIRTY: 3% have lowercase segment ("sme")
    seg[dirty] = seg[dirty].str.lower()
    managers = [fake.name() for _ in range(12)]
    created = START - pd.to_timedelta(rng.integers(30, 400, n), unit="D")
    return pd.DataFrame({
        "customer_id": np.arange(1, n + 1),
        "customer_code": [f"C{i:05d}" for i in range(1, n + 1)],
        "customer_name": [fake.company() for _ in range(n)],
        "segment": seg,
        "account_manager": rng.choice(managers, n),
        "region": rng.choice(["North", "South", "East", "West"], n, p=[0.30, 0.30, 0.15, 0.25]),
        "payment_terms_days": rng.choice([15, 30, 45, 60], n),
        "status": rng.choice(["ACTIVE", "INACTIVE"], n, p=[0.97, 0.03]),
        "created_at": created,
        "updated_at": created,   # account-manager reassignments arrive in the increment (SCD2 demo)
    })


# ------------------------------------------------------------------ shipments = the ground truth
SHIP_COLS = ["shipment_id", "shipment_ref", "customer_id", "carrier_id", "origin_location_id",
             "dest_location_id", "service_level", "booked_at", "pickup_planned_at",
             "promised_delivery_at", "weight_kg", "volume_cbm", "quoted_carrier_cost", "currency",
             "status", "created_at", "updated_at"]   # only these go to Postgres; truth columns stay out


def build_shipments(customers, locations):
    n = N_SHIPMENTS
    span_s = int((CUTOFF - START).total_seconds())
    # All timestamps are floored to the minute, so the "dd/mm/yyyy HH:MM" scan format loses nothing.
    booked_at = pd.Series(START + pd.to_timedelta(rng.integers(0, span_s, n), unit="s")).dt.floor("min")
    booked_at = booked_at.sort_values().reset_index(drop=True)      # ids increase with time

    # SKEW: a few customers ship far more than the rest (Zipf-like weights). This is what causes
    # slow, uneven Spark tasks in the performance lab (Stage 10).
    w = 1.0 / np.arange(1, N_CUSTOMERS + 1) ** 0.8
    w = rng.permutation(w / w.sum())
    customer_id = rng.choice(customers.customer_id.values, n, p=w)

    loc_ids = locations.location_id.values
    hub_ids = locations.loc[locations.location_type == "HUB", "location_id"].values
    origin = rng.choice(hub_ids, n)                  # freight starts at one of our hubs
    dest = rng.choice(loc_ids, n)
    same = dest == origin
    dest[same] = dest[same] % len(loc_ids) + 1       # guarantee origin != destination
    region = locations.set_index("location_id").region
    long_haul = pd.Series(origin).map(region).values != pd.Series(dest).map(region).values

    service = rng.choice(["STANDARD", "EXPRESS", "ECONOMY"], n, p=[0.60, 0.25, 0.15])
    svc_days = pd.Series(service).map({"EXPRESS": 2, "STANDARD": 4, "ECONOMY": 7}).values
    promised = booked_at + pd.Timedelta(days=1) + td(svc_days + long_haul.astype(int), "D")

    carrier_id = rng.choice([1, 2, 3], n, p=[0.45, 0.35, 0.20])
    late = rng.random(n) < LATE_PROB[carrier_id - 1]
    # Late: at least 2h late with a long exponential tail. On time: about 10h early, capped so
    # "late" is exactly (delivered > promised) with no fuzzy boundary.
    delay_h = np.where(late, rng.exponential(30, n) + 2, np.minimum(rng.normal(-10, 5, n), -0.5))
    delivered_at = (promised + td(delay_h, "h")).dt.floor("min")
    pickup_at = (booked_at + td(rng.uniform(3, 20, n), "h")).dt.floor("min")

    weight = np.clip(rng.lognormal(6.5, 1.0, n), 20, 25000).round(2)
    rate = pd.Series(service).map({"ECONOMY": 6, "STANDARD": 8, "EXPRESS": 12}).values
    quoted = (400 + weight * rate * np.where(long_haul, 1.5, 1.0)).round(2)   # always INR

    status = np.where(delivered_at <= CUTOFF, "DELIVERED",
                      np.where(pickup_at <= CUTOFF, "IN_TRANSIT", "BOOKED"))
    status = np.where(rng.random(n) < 0.02, "CANCELLED", status)

    df = pd.DataFrame({
        "shipment_id": np.arange(1, n + 1),
        "shipment_ref": "MFL-" + booked_at.dt.year.astype(str) + "-"
                        + pd.Series(np.arange(1, n + 1)).astype(str).str.zfill(7),
        "customer_id": customer_id, "carrier_id": carrier_id,
        "origin_location_id": origin, "dest_location_id": dest,
        "service_level": service, "booked_at": booked_at,
        "pickup_planned_at": booked_at.dt.normalize() + pd.Timedelta(days=1, hours=9),
        "promised_delivery_at": promised,
        "weight_kg": weight, "volume_cbm": (weight / rng.uniform(150, 250, n)).round(2),
        "quoted_carrier_cost": quoted,
        "currency": np.where(rng.random(n) < 0.05, "USD", "INR"),   # 5% export shipments billed in USD
        "status": status, "created_at": booked_at,
        # TRUTH columns. The TMS never stores the real delivery time; it only exists in the scans.
        "pickup_at": pickup_at, "delivered_at": delivered_at,
        "quoted_true": quoted, "promised_true": promised,
    })

    # updated_at = when the row last changed. Stage 8's watermark reads this column.
    upd = df.booked_at.copy()
    m = df.status == "DELIVERED"
    upd[m] = df.delivered_at[m]
    m = df.status == "IN_TRANSIT"
    upd[m] = (CUTOFF - pd.to_timedelta(rng.uniform(0, 48, m.sum()), unit="h")).to_numpy()
    m = df.status == "CANCELLED"
    upd[m] = df.booked_at[m] + pd.Timedelta(hours=2)
    df["updated_at"] = upd.clip(lower=df.booked_at, upper=CUTOFF)

    # ---- DIRTY DATA, injected AFTER the truth columns were copied
    df.loc[rng.random(n) < 0.01, "weight_kg"] = np.nan                       # missing weight (1%)
    neg = rng.random(n) < 0.002
    df.loc[neg, "weight_kg"] = -df.loc[neg, "weight_kg"].abs()               # impossible negative weight
    bad = rng.random(n) < 0.001
    df.loc[bad, "promised_delivery_at"] = df.loc[bad, "booked_at"] - pd.Timedelta(days=1)  # promised BEFORE booked
    df.loc[rng.random(n) < 0.005, "quoted_carrier_cost"] = np.nan            # missing quote (0.5%)
    return df


def build_charges(ship):
    """What we BILL the customer, one row per charge: freight + fuel surcharge (+ sometimes detention)."""
    s = ship[ship.status != "CANCELLED"]
    margin = rng.normal(0.18, 0.05, len(s)).clip(0.02, 0.40)   # each shipment has its own margin
    freight = s.quoted_true.values * (1 + margin)
    fuel = freight * 0.08
    det = rng.random(len(s)) < 0.08
    frames = []
    for ctype, rows, amt in [("FREIGHT", s, freight), ("FUEL_SURCHARGE", s, fuel),
                             ("DETENTION", s[det], freight[det] * 0.10)]:
        amt = pd.Series(amt, index=rows.index)
        usd = rows.currency == "USD"
        amt[usd] = amt[usd] / USD_INR                # export shipments are billed in USD
        frames.append(pd.DataFrame({"shipment_id": rows.shipment_id, "charge_type": ctype,
                                    "amount": amt.round(2), "currency": rows.currency,
                                    "updated_at": rows.booked_at}))
    out = pd.concat(frames, ignore_index=True).sort_values(["shipment_id", "charge_type"]).reset_index(drop=True)
    out.insert(0, "charge_id", np.arange(1, len(out) + 1))
    return out


def build_claims(ship):
    d = ship[ship.status == "DELIVERED"].sample(frac=0.015, random_state=SEED)   # ~1.5% of deliveries
    opened = (d.delivered_at + td(rng.integers(1, 10, len(d)), "D")).clip(upper=CUTOFF)
    out = pd.DataFrame({
        "shipment_id": d.shipment_id.values,
        "claim_type": rng.choice(["DAMAGE", "LOSS", "DELAY"], len(d), p=[0.5, 0.2, 0.3]),
        "claim_amount": rng.lognormal(8, 0.8, len(d)).round(2),
        "status": rng.choice(["OPEN", "SETTLED", "REJECTED"], len(d), p=[0.3, 0.5, 0.2]),
        "opened_at": opened.values, "updated_at": opened.values})
    out.insert(0, "claim_id", np.arange(1, len(out) + 1))
    return out


# ------------------------------------------------------------------ Postgres load
def load_postgres(tables):
    eng = create_engine(DB_URL)
    with eng.begin() as c:   # TRUNCATE makes the generator safely re-runnable
        c.execute(text("TRUNCATE tms.claims, tms.shipment_charges, tms.shipments, tms.customers, "
                       "tms.carriers, tms.locations RESTART IDENTITY CASCADE"))
    for name, df in tables:  # parents before children because of foreign keys
        df.to_sql(name, eng, schema="tms", if_exists="append", index=False, method="multi", chunksize=5000)
        print(f"  loaded tms.{name}: {len(df):,} rows")
    with eng.begin() as c:   # we inserted explicit ids, so move each auto-increment sequence past them
        for t, pk in [("locations", "location_id"), ("customers", "customer_id"), ("carriers", "carrier_id"),
                      ("shipments", "shipment_id"), ("shipment_charges", "charge_id"), ("claims", "claim_id")]:
            c.execute(text(f"SELECT setval(pg_get_serial_sequence('tms.{t}','{pk}'), "
                           f"(SELECT MAX({pk}) FROM tms.{t}))"))


# ------------------------------------------------------------------ tracking scans -> Parquet
# (event type, fraction of the journey elapsed, where: O=origin hub, D=destination, X=random checkpoint)
EVENTS = [("PICKED_UP", 0.00, "O"), ("HUB_IN", 0.05, "O"), ("HUB_OUT", 0.15, "O"),
          ("CHECKPOINT", 0.30, "X"), ("CHECKPOINT", 0.45, "X"), ("CHECKPOINT", 0.60, "X"),
          ("CHECKPOINT", 0.75, "X"), ("HUB_IN", 0.88, "D"), ("OUT_FOR_DELIVERY", 0.95, "D"),
          ("DELIVERED", 1.00, "D")]


def write_scans(ship, locations):
    s = ship[ship.status != "CANCELLED"].reset_index(drop=True)
    code = locations.set_index("location_id").location_code
    o_code, d_code = s.origin_location_id.map(code).values, s.dest_location_id.map(code).values
    all_codes = locations.location_code.values
    dur = s.delivered_at - s.pickup_at

    frames = []
    for etype, frac, where in EVENTS:
        ts = s.pickup_at + dur * frac
        if etype != "DELIVERED":                     # DELIVERED stays exact = the truth
            ts = ts + td(rng.integers(-20, 20, len(s)), "min")
        ts = ts.dt.floor("min")
        loc = o_code if where == "O" else d_code if where == "D" else rng.choice(all_codes, len(s))
        frames.append(pd.DataFrame({"shipment_ref": s.shipment_ref.values, "event_type": etype,
                                    "event_ts": ts.values, "location_code": loc}))
    ev = pd.concat(frames, ignore_index=True)

    # MESS: 1% of DELIVERED scans never happen (TMS may say delivered, but no scan exists)
    ev = ev[~((ev.event_type == "DELIVERED") & (rng.random(len(ev)) < 0.01))].reset_index(drop=True)
    n = len(ev)

    # LATE ARRIVAL: 10% of scans reach us 1-5 days after they happened. Files are named by ARRIVAL date.
    lag = np.where(rng.random(n) < 0.10, rng.integers(1, 6, n), 0)
    ev["arrival_date"] = ev.event_ts.dt.normalize() + td(lag, "D")
    ev["scan_source"] = rng.choice(["HANDHELD", "HUB_SCANNER", "DRIVER_APP"], n)

    # MESS: three timestamp formats stored as strings. dd/mm/yyyy is the classic trap
    # (is 03/04/2025 March 4th or April 3rd?)
    ts, r = ev.event_ts, rng.random(n)
    out = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    m = (r >= 0.80) & (r < 0.95)
    out[m] = ts[m].dt.strftime("%d/%m/%Y %H:%M")
    m = r >= 0.95
    out[m] = ts[m].dt.strftime("%Y-%m-%dT%H:%M:%S")
    ev["event_ts"] = out

    # MESS: shipment_ref in a second format (5%): MFL-2025-0000123 -> MFL20250000123
    m = rng.random(n) < 0.05
    ev.loc[m, "shipment_ref"] = ev.loc[m, "shipment_ref"].str.replace("-", "", regex=False)
    # MESS: orphan scans for shipments the TMS has never heard of (0.2%)
    m = rng.random(n) < 0.002
    ev.loc[m, "shipment_ref"] = ["MFL-2026-" + str(x) for x in rng.integers(9_000_000, 9_999_999, m.sum())]
    # MESS: lowercase location codes (1%) and unknown codes (0.5%)
    m = rng.random(n) < 0.01
    ev.loc[m, "location_code"] = ev.loc[m, "location_code"].str.lower()
    m = rng.random(n) < 0.005
    ev.loc[m, "location_code"] = "XX99"
    # MESS: missing event type (0.2%)
    ev.loc[rng.random(n) < 0.002, "event_type"] = None

    # MESS: exact duplicate scans (2%). Half arrive again the NEXT day in another file
    # (a scanner resending its buffer).
    dup = ev.sample(frac=0.02, random_state=SEED).copy()
    dup["arrival_date"] = dup.arrival_date + td((rng.random(len(dup)) < 0.5).astype(int), "D")
    ev = pd.concat([ev, dup], ignore_index=True)

    # Split at the cutoff: arrived on or before 31 May -> raw files. Later -> held back for Stage 8.
    cut_day = CUTOFF.normalize()
    past, future = ev[ev.arrival_date <= cut_day], ev[ev.arrival_date > cut_day]
    scans_dir = RAW / "scans"
    scans_dir.mkdir(parents=True, exist_ok=True)
    HELD.mkdir(parents=True, exist_ok=True)
    files = 0
    for d, g in past.groupby("arrival_date"):
        g = g.drop(columns=["arrival_date"])
        if d < pd.Timestamp("2025-04-01"):          # SCHEMA DRIFT: early files have no scan_source column
            g = g.drop(columns=["scan_source"])
        g.to_parquet(scans_dir / f"scans_{d:%Y%m%d}.parquet", index=False)
        files += 1
    future.to_parquet(HELD / "scans_future.parquet", index=False)
    return {"scan_rows_written": len(past), "scan_files": files, "scan_rows_held_back": len(future)}


# ------------------------------------------------------------------ carrier invoices -> 3 CSV layouts
def layout_blz(g):     # BlueLine: comma CSV, ISO dates, dashed refs
    return pd.DataFrame({
        "InvoiceNo": "BLZ-" + g.inv_date.dt.strftime("%Y%m%d"),
        "InvoiceDate": g.inv_date.dt.strftime("%Y-%m-%d"),
        "ShipmentRef": g.shipment_ref,
        "ChargeType": g.line_type.map({"FREIGHT": "Freight", "FUEL": "Fuel", "DETENTION": "Detention"}),
        "Amount": g.amount, "Currency": "INR"})


def layout_swf(g):     # SwiftHaul: SEMICOLON csv, dd-mm-yyyy, refs WITHOUT dashes, decimal COMMA
    return pd.DataFrame({
        "inv_id": "SWF-" + g.inv_date.dt.strftime("%Y%m%d"),
        "date": g.inv_date.dt.strftime("%d-%m-%Y"),
        "ref": g.shipment_ref.str.replace("-", "", regex=False),
        "description": g.line_type.map({"FREIGHT": "Base freight", "FUEL": "Fuel surcharge",
                                        "DETENTION": "Detention"}),
        "amount_inr": g.amount})


def layout_trk(g):     # TruckNet: dd-Mon-yy dates, line-type codes, thousands separators, NET + GST split
    return pd.DataFrame({
        "Invoice Number": "TRK/" + g.inv_date.dt.strftime("%Y%m%d"),
        "Inv Date": g.inv_date.dt.strftime("%d-%b-%y"),
        "Consignment": g.shipment_ref,
        "Line Type": g.line_type.map({"FREIGHT": "F", "FUEL": "FS", "DETENTION": "D"}),
        "Net Amt": g.amount.map("{:,.2f}".format),
        "GST Amt": (g.amount * 0.18).map("{:,.2f}".format)})


LAYOUTS = {1: ("BLZ", layout_blz, {}), 2: ("SWF", layout_swf, {"sep": ";", "decimal": ","}),
           3: ("TRK", layout_trk, {})}


def write_invoices(ship):
    d = ship[ship.status == "DELIVERED"].copy()      # carriers invoice after delivery
    n = len(d)
    over = rng.random(n) < 0.06                      # 6% of shipments are OVERCHARGED by 10-25%
    factor = np.where(over, rng.uniform(1.10, 1.25, n), rng.normal(1.0, 0.015, n))
    total = d.quoted_true.values * factor
    d["inv_date"] = d.delivered_at.dt.normalize() + td(rng.integers(3, 26, n), "D")

    base = pd.DataFrame({"shipment_id": d.shipment_id.values, "shipment_ref": d.shipment_ref.values,
                         "carrier_id": d.carrier_id.values, "inv_date": d.inv_date.values})
    lines = []
    for ltype, share in [("FREIGHT", 0.88), ("FUEL", 0.12)]:
        x = base.copy()
        x["line_type"], x["amount"] = ltype, (total * share).round(2)
        lines.append(x)
    det = rng.random(n) < 0.08                       # 8% get a DETENTION charge that was never in the quote
    x = base[det].copy()
    x["line_type"], x["amount"] = "DETENTION", rng.uniform(500, 2500, det.sum()).round(2)
    lines.append(x)
    L = pd.concat(lines, ignore_index=True)
    L = L[L.inv_date <= CUTOFF].reset_index(drop=True)   # later invoices arrive in the increment
    inv_true = L.groupby("shipment_id").amount.sum()     # the TRUE carrier cost per shipment (before mess)

    # MESS: duplicate invoice lines (3%) and "ghost" lines that match no shipment (0.5%)
    dups = L.sample(frac=0.03, random_state=SEED)
    ghost = L.sample(frac=0.005, random_state=SEED + 1).copy()
    ghost["shipment_ref"] = ["MFL-2026-" + str(x) for x in rng.integers(9_000_000, 9_999_999, len(ghost))]
    L2 = pd.concat([L, dups, ghost], ignore_index=True).sort_values("inv_date").reset_index(drop=True)

    files = 0
    for cid, (code, fn, kw) in LAYOUTS.items():
        sub = L2[L2.carrier_id == cid]
        folder = RAW / "invoices" / code
        folder.mkdir(parents=True, exist_ok=True)
        for month, g in sub.groupby(sub.inv_date.dt.strftime("%Y-%m")):
            fn(g).to_csv(folder / f"invoices_{code}_{month}.csv", index=False, **kw)
            files += 1
    return inv_true, {"invoice_lines_written": len(L2), "invoice_files": files}


# ------------------------------------------------------------------ main
def main():
    for p in (RAW, TRUTH, HELD):
        p.mkdir(parents=True, exist_ok=True)

    print("1/5 building reference data + shipments...")
    locations, carriers, customers = build_locations(), build_carriers(), build_customers()
    ship = build_shipments(customers, locations)
    charges, claims = build_charges(ship), build_claims(ship)

    print("2/5 loading Postgres (about a minute)...")
    load_postgres([("locations", locations), ("customers", customers), ("carriers", carriers),
                   ("shipments", ship[SHIP_COLS]), ("shipment_charges", charges), ("claims", claims)])

    print("3/5 writing tracking scan files...")
    scan_stats = write_scans(ship, locations)

    print("4/5 writing carrier invoice files...")
    inv_true, inv_stats = write_invoices(ship)

    print("5/5 writing the truth table...")
    charges["inr"] = charges.amount * np.where(charges.currency == "USD", USD_INR, 1.0)
    billed = charges.groupby("shipment_id").inr.sum()
    truth = ship.loc[ship.status != "CANCELLED",
                     ["shipment_id", "shipment_ref", "customer_id", "carrier_id", "service_level",
                      "status", "promised_true", "pickup_at", "delivered_at", "quoted_true"]].copy()
    truth["on_time"] = truth.delivered_at <= truth.promised_true       # only meaningful when DELIVERED
    truth["billed_inr"] = truth.shipment_id.map(billed)
    truth["carrier_invoiced_true"] = truth.shipment_id.map(inv_true)
    truth.to_parquet(TRUTH / "shipment_truth.parquet", index=False)

    dl = truth[truth.status == "DELIVERED"]
    print("\n=== DONE ===")
    print(f"shipments: {len(ship):,} | charges: {len(charges):,} | claims: {len(claims):,}")
    print(scan_stats, inv_stats)
    print("TRUE on-time % by carrier (1=BlueLine, 2=SwiftHaul, 3=TruckNet):")
    print((dl.groupby("carrier_id").on_time.mean() * 100).round(1).to_string())
    print(f"TRUE overall on-time %: {dl.on_time.mean() * 100:.1f}")


if __name__ == "__main__":
    main()
