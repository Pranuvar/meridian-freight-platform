"""Data-quality rules. A Rule describes what a BAD row looks like."""
from dataclasses import dataclass

from pyspark.sql import Column
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    fail: Column                 # evaluates to TRUE when the row VIOLATES the rule
    severity: str = "reject"     # "reject" -> quarantined | "warn" -> row kept, problem recorded
    description: str = ""


# ---------------------------------------------------------------- generic building blocks
def not_null(column: str, severity: str = "reject") -> Rule:
    c = F.col(column)
    # "isNull() | ..." is TRUE whenever the value is null, even though the right side would be NULL.
    return Rule(f"{column}_missing", c.isNull() | (F.trim(c.cast("string")) == ""), severity,
                f"{column} is null or blank")


def not_in_set(column: str, allowed: list, severity: str = "reject") -> Rule:
    c = F.col(column)
    return Rule(f"{column}_invalid", c.isNotNull() & ~c.isin(allowed), severity,
                f"{column} not in {allowed}")


# ---------------------------------------------------------------- shared parsing helpers
# The scanning system writes timestamps in three formats. dd/MM/yyyy is read day-first, never
# month-first: 04/03/2025 is 4 March.
TS_FORMATS = ["yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm", "yyyy-MM-dd'T'HH:mm:ss"]


def parse_event_ts(column: str = "event_ts") -> Column:
    """Try each format in turn. to_timestamp returns NULL on a non-match, coalesce takes the first hit."""
    return F.coalesce(*[F.to_timestamp(F.col(column), f) for f in TS_FORMATS])


def normalise_ref(col: Column) -> Column:
    """MFL20250000123 or MFL-2025-0000123 -> MFL-2025-0000123 (the standard form)."""
    return F.regexp_replace(col, r"^MFL-?(\d{4})-?(\d{7})$", "MFL-$1-$2")


# ---------------------------------------------------------------- rule sets per table
def shipment_rules() -> list:
    return [
        # REJECT: the row cannot feed the on-time KPI or a sane weight-based measure.
        Rule("promised_before_booked", F.col("promised_delivery_at") < F.col("booked_at"), "reject",
             "SLA promise earlier than booking"),
        Rule("weight_non_positive", F.col("weight_kg") <= 0, "reject", "impossible weight"),
        # WARN: the shipment is still valid for delivery KPIs; only cost-per-kg style measures suffer.
        not_null("weight_kg", "warn"),
        not_null("quoted_carrier_cost", "warn"),
    ]


def customer_rules() -> list:
    return [
        not_in_set("segment", ["Enterprise", "Mid-Market", "SME"], "warn"),   # fixed by standardising
        not_in_set("status", ["ACTIVE", "INACTIVE"], "reject"),
    ]


def scan_rules(valid_location_codes: list) -> list:
    ref = F.col("shipment_ref")
    loc = F.col("location_code")
    return [
        not_null("event_type", "reject"),
        Rule("event_ts_unparseable", parse_event_ts().isNull(), "reject", "matches none of the 3 formats"),
        Rule("shipment_ref_bad_format", ref.isNull() | ~ref.rlike(r"^MFL-?\d{4}-?\d{7}$"), "reject",
             "not a shipment reference in any known format"),
        Rule("shipment_ref_nonstandard", ref.rlike(r"^MFL\d{4}\d{7}$"), "warn", "missing dashes"),
        # After upper/trim the code must exist in the location reference, else Gold uses "Unknown".
        Rule("location_unknown", loc.isNull() | ~F.upper(F.trim(loc)).isin(valid_location_codes),
             "warn", "location code not in reference data"),
        Rule("location_case", loc != F.upper(loc), "warn", "lowercase location code"),
    ]
