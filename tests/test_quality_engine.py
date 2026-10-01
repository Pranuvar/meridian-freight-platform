from pyspark.sql import functions as F

from src.quality.engine import measure, split
from src.quality.rules import Rule, not_null, parse_event_ts, scan_rules


def test_split_conserves_rows_and_handles_nulls(spark):
    df = spark.createDataFrame([(1, 10.0), (2, None), (3, -5.0)], "id int, weight_kg double")
    rules = [Rule("weight_non_positive", F.col("weight_kg") <= 0), not_null("weight_kg", "warn")]
    clean, rejected = split(df, rules)
    # Nothing may vanish: clean + rejected == input.
    assert clean.count() + rejected.count() == df.count()
    # Only the negative weight is rejected. NULL <= 0 is NULL, so the null row is NOT rejected...
    assert [r["id"] for r in rejected.collect()] == [3]
    # ...but the separate not_null rule flags it as a warning, and the row stays.
    warns = {r["id"]: r["_dq_warn_reasons"] for r in clean.collect()}
    assert warns[2] == ["weight_kg_missing"] and warns[1] == []


def test_timestamp_formats_are_read_day_first(spark):
    df = spark.createDataFrame(
        [("2025-03-04 10:15:00",), ("04/03/2025 10:15",), ("2025-03-04T10:15:00",), ("garbage",)],
        "event_ts string")
    parsed = [r["ts"] for r in df.select(parse_event_ts().alias("ts")).collect()]
    assert parsed[0] == parsed[1] == parsed[2]     # 04/03/2025 is 4 March, not 3 April
    assert parsed[3] is None                        # unparseable becomes NULL, not an exception


def test_scan_rules_flag_the_right_rows(spark):
    df = spark.createDataFrame([
        ("MFL-2025-0000123", "PICKED_UP", "2025-03-04 10:15:00", "DEL01"),   # clean
        ("MFL20250000123", "PICKED_UP", "2025-03-04 10:15:00", "del01"),     # no dashes + lowercase code
        ("BADREF", "PICKED_UP", "2025-03-04 10:15:00", "DEL01"),             # invalid reference
        ("MFL-2025-0000123", None, "2025-03-04 10:15:00", "XX99"),           # no event type + unknown code
    ], "shipment_ref string, event_type string, event_ts string, location_code string")
    n, rejected, warned, results = measure(df, scan_rules(["DEL01"]))
    got = {name: cnt for name, _, cnt in results}
    assert (n, rejected, warned) == (4, 2, 2)
    assert got["shipment_ref_bad_format"] == 1
    assert got["shipment_ref_nonstandard"] == 1
    assert got["event_type_missing"] == 1
    assert got["location_unknown"] == 1     # only XX99. "del01" is valid after upper()
    assert got["location_case"] == 1
