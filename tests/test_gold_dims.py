from datetime import date, datetime

from src.gold.build_dims import build_dim_date
from src.gold.scd2 import initial_load, merge_changes

COLS = ("customer_id int, customer_code string, customer_name string, segment string, "
        "account_manager string, region string, payment_terms_days int, status string, updated_at timestamp")


def test_dim_date_fiscal_year_and_iso_fields(spark):
    rows = build_dim_date(spark, "2026-03-31", "2026-04-05").collect()
    d = {r["date"].isoformat(): r for r in rows}
    assert d["2026-03-31"]["fiscal_year"] == "FY2025-26" and d["2026-03-31"]["fiscal_quarter"] == 4
    assert d["2026-04-01"]["fiscal_year"] == "FY2026-27" and d["2026-04-01"]["fiscal_quarter"] == 1
    assert d["2026-04-01"]["date_key"] == 20260401
    assert d["2026-04-01"]["day_of_week"] == 3 and d["2026-04-01"]["day_name"] == "Wednesday"
    assert d["2026-04-04"]["is_weekend"] is True and d["2026-04-01"]["is_weekend"] is False


def test_scd2_lifecycle(spark, tmp_path):
    path = str(tmp_path / "dim_customer")
    t0 = datetime(2025, 1, 1)
    v1 = [(1, "C1", "Acme", "SME", "Asha", "North", 30, "ACTIVE", t0),
          (2, "C2", "Beta", "SME", "Ravi", "South", 30, "ACTIVE", t0)]
    initial_load(spark, path, spark.createDataFrame(v1, COLS))
    dim = lambda: spark.read.format("delta").load(path)          # noqa: E731
    assert dim().filter("customer_id > 0").count() == 2
    assert dim().filter("customer_id = -1").count() == 1           # the unknown member

    # 1) Nothing changed -> nothing happens (idempotent).
    assert merge_changes(spark, path, spark.createDataFrame(v1, COLS)) == \
        {"new_customers": 0, "new_versions": 0, "type1_updates": 0}

    # 2) Customer 1 changes manager (Type 2). Customer 2 is renamed (Type 1). Customer 3 is new.
    v2 = [(1, "C1", "Acme", "SME", "Meera", "North", 30, "ACTIVE", datetime(2026, 4, 1, 9)),
          (2, "C2", "Beta Ltd", "SME", "Ravi", "South", 30, "ACTIVE", t0),
          (3, "C3", "Gamma", "Enterprise", "Ravi", "West", 45, "ACTIVE", t0)]
    assert merge_changes(spark, path, spark.createDataFrame(v2, COLS)) == \
        {"new_customers": 1, "new_versions": 1, "type1_updates": 1}
    c1 = dim().filter("customer_id = 1").orderBy("effective_from").collect()
    assert [r["account_manager"] for r in c1] == ["Asha", "Meera"]
    assert c1[0]["effective_to"] == date(2026, 4, 1) == c1[1]["effective_from"]     # no gap, no overlap
    assert [r["is_current"] for r in c1] == [False, True]
    assert c1[1]["effective_to"] == date(9999, 12, 31)
    assert len({r["customer_key"] for r in c1}) == 2                                 # one key per version
    assert {r["customer_name"] for r in dim().filter("customer_id = 2").collect()} == {"Beta Ltd"}
    assert dim().filter("customer_id = 3").count() == 1
    assert dim().filter("is_current").groupBy("customer_id").count().filter("count > 1").count() == 0

    # 3) Customer 1 changes AGAIN, stamped the same day the current version started.
    v3 = [(1, "C1", "Acme", "SME", "Priya", "North", 30, "ACTIVE", datetime(2026, 4, 1, 15))] + v2[1:]
    merge_changes(spark, path, spark.createDataFrame(v3, COLS))
    c1 = dim().filter("customer_id = 1").orderBy("effective_from").collect()
    assert len(c1) == 3
    assert c1[2]["effective_from"] == date(2026, 4, 2)          # pushed to the next day, so no zero-length version
