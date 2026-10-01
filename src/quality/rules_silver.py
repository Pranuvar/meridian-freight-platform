"""Extra rule sets used by the Silver jobs (parents, charges, claims, invoices)."""
from pyspark.sql import functions as F

from src.quality.rules import Rule, not_in_set, not_null


def orphan_rule(marker: str = "_known_ref", name: str = "orphan_no_shipment") -> Rule:
    """The job left-joins a table of known keys and adds `marker` (NULL when there was no match)."""
    return Rule(name, F.col(marker).isNull(), "reject", "no matching parent record")


def charge_rules() -> list:
    return [
        Rule("amount_non_positive", F.col("amount") <= 0, "reject", "customer charge must be > 0"),
        not_null("currency", "reject"),
        not_in_set("currency", ["INR", "USD"], "reject"),
        orphan_rule("_known_parent", "parent_shipment_missing"),
    ]


def claim_rules() -> list:
    return [
        Rule("claim_amount_non_positive", F.col("claim_amount") <= 0, "reject"),
        not_in_set("claim_type", ["DAMAGE", "LOSS", "DELAY"], "reject"),
        not_in_set("status", ["OPEN", "SETTLED", "REJECTED"], "reject"),
        orphan_rule("_known_parent", "parent_shipment_missing"),
    ]


def invoice_rules() -> list:
    """Applied AFTER the three layouts are parsed into one schema, so a NULL here means 'failed to parse'."""
    return [
        Rule("invoice_date_invalid", F.col("invoice_date").isNull(), "reject", "unparseable invoice date"),
        Rule("amount_invalid", F.col("amount").isNull(), "reject", "unparseable amount"),
        Rule("amount_non_positive", F.col("amount") <= 0, "reject", "invoice line must be > 0"),
        Rule("charge_type_unknown", F.col("charge_type").isNull(), "reject", "unmapped charge type"),
        orphan_rule("_known_ref", "orphan_no_shipment"),
    ]
