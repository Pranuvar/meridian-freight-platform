"""Silver: TMS tables. JDBC already gave correct types, so the work here is dedupe, standardise,
validate, quarantine. One generic process() handles every table."""
from pyspark.sql import functions as F

from src.bronze.common import new_batch_id
from src.quality.engine import reset_quarantine, split, write_quarantine
from src.quality.rules import customer_rules, shipment_rules
from src.quality.rules_silver import charge_rules, claim_rules
from src.silver.common import account, dedupe, silver_path
from src.utils.config import bronze_path
from src.utils.spark import get_spark


def process(spark, batch, table, df, keys, order_by, rules, transform=None, helper_cols=()):
    n_in = df.count()
    # persist(): this DataFrame feeds several actions below. Without it, each one would re-run
    # the whole plan, including the window shuffle.
    deduped = dedupe(df, keys, order_by).persist()
    n_dups = n_in - deduped.count()

    clean, rejected = split(deduped, rules)
    if helper_cols:
        clean, rejected = clean.drop(*helper_cols), rejected.drop(*helper_cols)
    if transform:
        clean = transform(clean)
    clean = (clean.withColumn("_silver_batch_id", F.lit(batch))
                  .withColumn("_silver_processed_at", F.current_timestamp()))

    out = silver_path(table)
    clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)

    n_rej = rejected.count()
    reset_quarantine(spark, table)
    if n_rej:
        write_quarantine(rejected, table, "tms_postgres", batch)
    account(spark, batch, table, n_in, n_dups, n_rej, out)
    deduped.unpersist()


# ---------------------------------------------------------------- standardisation per table
def std_locations(df):
    return (df.withColumn("location_code", F.upper(F.trim("location_code")))
              .withColumn("region", F.trim("region")))


def std_carriers(df):
    return df.withColumn("carrier_code", F.upper(F.trim("carrier_code")))


def std_customers(df):
    seg = F.upper(F.trim("segment"))
    return df.withColumn("segment",
                         F.when(seg == "ENTERPRISE", "Enterprise")
                          .when(seg == "MID-MARKET", "Mid-Market")
                          .when(seg == "SME", "SME")
                          .otherwise(F.col("segment")))      # unknown values stay visible


def std_shipments(df):
    return (df.withColumn("status", F.upper(F.trim("status")))
              .withColumn("service_level", F.upper(F.trim("service_level")))
              .withColumn("currency", F.upper(F.trim("currency"))))


def std_charges(df):
    return (df.withColumn("charge_type", F.upper(F.trim("charge_type")))
              .withColumn("currency", F.upper(F.trim("currency"))))


def std_claims(df):
    return (df.withColumn("claim_type", F.upper(F.trim("claim_type")))
              .withColumn("status", F.upper(F.trim("status"))))


def main():
    spark = get_spark("silver_tms")
    batch = new_batch_id("initial")

    def b(name):
        return spark.read.format("delta").load(bronze_path(name))

    ingested = [F.col("_ingested_at").desc()]
    latest = [F.col("updated_at").desc(), F.col("_ingested_at").desc()]

    process(spark, batch, "locations", b("tms_locations"), ["location_id"], ingested, [], std_locations)
    process(spark, batch, "carriers", b("tms_carriers"), ["carrier_id"], latest, [], std_carriers)
    process(spark, batch, "customers", b("tms_customers"), ["customer_id"], latest,
            customer_rules(), std_customers)
    process(spark, batch, "shipments", b("tms_shipments"), ["shipment_id"], latest,
            shipment_rules(), std_shipments)

    # Child tables: mark rows whose parent shipment survived into Silver. The parent table is
    # small (200k ids), so broadcast it: every task gets a copy and the big side never shuffles.
    known = (spark.read.format("delta").load(silver_path("shipments"))
             .select("shipment_id").distinct().withColumn("_known_parent", F.lit(1)))
    charges = b("tms_shipment_charges").join(F.broadcast(known), "shipment_id", "left")
    process(spark, batch, "shipment_charges", charges, ["charge_id"], latest,
            charge_rules(), std_charges, helper_cols=["_known_parent"])
    claims = b("tms_claims").join(F.broadcast(known), "shipment_id", "left")
    process(spark, batch, "claims", claims, ["claim_id"], latest,
            claim_rules(), std_claims, helper_cols=["_known_parent"])
    spark.stop()


if __name__ == "__main__":
    main()
