from datetime import date
from decimal import Decimal

from src.gold.build_facts import attach_customer_version, fx_daily, key_lookup


def test_fx_weekend_uses_last_published_rate(spark):
    dates = spark.createDataFrame([(date(2025, 1, 3),), (date(2025, 1, 4),), (date(2025, 1, 5),), (date(2025, 1, 6),)],
                                  "date date")                                  # Fri, Sat, Sun, Mon
    fx = spark.createDataFrame([(date(2025, 1, 3), Decimal("84.1000")), (date(2025, 1, 6), Decimal("84.5000"))],
                               "rate_date date, rate decimal(10,4)")
    out = {r["date"]: r["usd_inr_rate"] for r in fx_daily(dates, fx).collect()}
    assert out[date(2025, 1, 4)] == Decimal("84.1000") == out[date(2025, 1, 5)]   # weekend carries Friday forward
    assert out[date(2025, 1, 6)] == Decimal("84.5000")


def test_customer_version_is_chosen_by_booking_date(spark):
    dim = spark.createDataFrame([
        (1, 100, date(1900, 1, 1), date(2026, 4, 1)),
        (1, 101, date(2026, 4, 1), date(9999, 12, 31)),
        (-1, -1, date(1900, 1, 1), date(9999, 12, 31))],
        "customer_id int, customer_key long, effective_from date, effective_to date")
    ship = spark.createDataFrame([("a", 1, date(2026, 3, 31)), ("b", 1, date(2026, 4, 1)),
                                  ("c", 1, date(2026, 4, 2)), ("d", 99, date(2026, 4, 2))],
                                 "shipment_ref string, customer_id int, booked_date date")
    result = attach_customer_version(ship, dim).collect()
    assert len(result) == 4                                    # the join must not multiply rows
    assert {r["shipment_ref"]: r["customer_key"] for r in result} == {"a": 100, "b": 101, "c": 101, "d": -1}


def test_missing_dimension_members_map_to_unknown(spark):
    dim = spark.createDataFrame([("DEL01", 11), ("UNKNOWN", -1)], "location_code string, location_key long")
    df = spark.createDataFrame([("DEL01",), ("XX99",), (None,)], "code string")
    out = key_lookup(df, dim, "code", "location_code", "location_key", "loc_key").collect()
    assert len(out) == 3 and sorted(r["loc_key"] for r in out) == [-1, -1, 11]
