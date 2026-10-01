"""Gold: the three fact tables. Full rebuild for now (Stage 10 turns this into incremental MERGE)."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.bronze.common import new_batch_id
from src.gold.common import UNKNOWN_KEY, gold_path, log_gold, sk
from src.silver.common import silver_path
from src.utils.spark import get_spark


def read_silver(spark, name):
    return spark.read.format("delta").load(silver_path(name))


def read_gold(spark, name):
    return spark.read.format("delta").load(gold_path(name))


def date_key(col: str):
    """Timestamp/date column -> int key like 20260401. NULL stays NULL (e.g. not delivered yet)."""
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


# ---------------------------------------------------------------- reusable, testable building blocks
def fx_daily(dates: DataFrame, fx: DataFrame) -> DataFrame:
    """One row per calendar date with the latest known USD->INR rate.
    Weekends and holidays have no published rate, so we carry the last known rate forward.
    dates: column `date`.  fx: columns `rate_date`, `rate`."""
    # last(..., ignorenulls=True) over a running window = 'most recent non-null value so far'.
    # No partitionBy: fine for ~1,500 calendar rows, a real problem on big data.
    w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    rates = fx.select(F.col("rate_date").alias("date"), F.col("rate").alias("_rate"))
    return (dates.select("date").join(rates, "date", "left")
            .withColumn("usd_inr_rate", F.last("_rate", ignorenulls=True).over(w))
            .select("date", "usd_inr_rate"))


def attach_customer_version(ship: DataFrame, dim_cust: DataFrame) -> DataFrame:
    """Adds customer_key = the dim_customer VERSION valid on the shipment's booking date (-1 if none).
    ship needs: customer_id, booked_date.  The dim is tiny, so it is broadcast to every task."""
    d = dim_cust.filter("customer_id <> -1").select(
        F.col("customer_id").alias("_cid"), F.col("customer_key").alias("_ckey"), "effective_from", "effective_to")
    cond = ((ship["customer_id"] == d["_cid"])
            & (ship["booked_date"] >= d["effective_from"])     # start is inclusive...
            & (ship["booked_date"] < d["effective_to"]))       # ...end is exclusive: exactly one version matches
    return (ship.join(F.broadcast(d), cond, "left")
            .withColumn("customer_key", F.coalesce(F.col("_ckey"), F.lit(UNKNOWN_KEY)))
            .drop("_cid", "_ckey", "effective_from", "effective_to"))


def key_lookup(df: DataFrame, dim: DataFrame, left_col: str, dim_code_col: str, dim_key_col: str,
               out_col: str) -> DataFrame:
    """Left-join a small dimension on its natural key; unmatched or NULL codes get the Unknown key (-1)."""
    d = dim.select(F.col(dim_code_col).alias("_code"), F.col(dim_key_col).alias("_key"))
    return (df.join(F.broadcast(d), df[left_col] == d["_code"], "left")
            .withColumn(out_col, F.coalesce(F.col("_key"), F.lit(UNKNOWN_KEY)))
            .drop("_code", "_key"))


def to_inr(native: str):
    """Native-currency amount x the FX rate applied to this shipment -> INR with 2 decimals. No charge = 0."""
    return (F.round(F.coalesce(F.col(native), F.lit(0)) * F.col("fx_rate_applied"), 2)
            .cast("decimal(14,2)"))


# ---------------------------------------------------------------- fact_shipment
def build_fact_shipment(spark, batch: str, refs: DataFrame = None) -> DataFrame:
    """refs=None rebuilds every shipment. refs=(shipment_id, shipment_ref) rebuilds ONLY those shipments:
    each source is narrowed to them BEFORE the aggregations, so the work scales with the change."""
    def scope(df, key):
        # left_semi keeps the rows of df whose key appears in refs. It adds no columns and never multiplies rows.
        return df if refs is None else df.join(F.broadcast(refs.select(key)), key, "left_semi")

    ship = scope(read_silver(spark, "shipments"), "shipment_ref")
    loc = read_silver(spark, "locations").select("location_id", "location_code")
    car = read_silver(spark, "carriers").select("carrier_id", "carrier_code")
    origin = loc.withColumnRenamed("location_id", "origin_location_id").withColumnRenamed("location_code", "origin_code")
    dest = loc.withColumnRenamed("location_id", "dest_location_id").withColumnRenamed("location_code", "dest_code")

    base = (ship.join(F.broadcast(origin), "origin_location_id", "left")
                .join(F.broadcast(dest), "dest_location_id", "left")
                .join(F.broadcast(car), "carrier_id", "left")
                .withColumn("booked_date", F.to_date("booked_at")))

    # Every many-rows-per-shipment source is collapsed to ONE row per shipment BEFORE joining.
    # Joining raw charges/scans/invoices would multiply shipment rows and inflate every measure.
    scans = scope(read_silver(spark, "scans"), "shipment_ref").groupBy("shipment_ref").agg(
        F.min(F.when(F.col("event_type") == "PICKED_UP", F.col("event_ts"))).alias("pickup_at"),
        F.min(F.when(F.col("event_type") == "DELIVERED", F.col("event_ts"))).alias("delivered_at"),
        F.count("*").alias("scan_count"))
    charges = scope(read_silver(spark, "shipment_charges"), "shipment_id").groupBy("shipment_id").agg(
        F.sum(F.when(F.col("charge_type") == "FREIGHT", F.col("amount"))).alias("freight_native"),
        F.sum(F.when(F.col("charge_type") == "FUEL_SURCHARGE", F.col("amount"))).alias("fuel_native"),
        F.sum(F.when(F.col("charge_type") == "DETENTION", F.col("amount"))).alias("detention_native"))
    invoices = scope(read_silver(spark, "carrier_invoice_lines"), "shipment_ref").groupBy("shipment_ref").agg(
        F.sum("amount").alias("carrier_cost_invoiced_inr"), F.count("*").alias("invoice_line_count"))
    claims = scope(read_silver(spark, "claims"), "shipment_id").groupBy("shipment_id").agg(
        F.count("*").alias("claim_count"), F.sum("claim_amount").alias("claim_amount_inr"))
    fx = fx_daily(read_gold(spark, "dim_date"),
                  read_silver(spark, "fx_rates").filter("base = 'USD' AND quote = 'INR'"))

    f = (base.join(scans, "shipment_ref", "left")
             .join(charges, "shipment_id", "left")
             .join(invoices, "shipment_ref", "left")
             .join(claims, "shipment_id", "left")
             .join(F.broadcast(fx.withColumnRenamed("date", "booked_date")), "booked_date", "left"))

    # Dimension keys. Unmatched -> -1 (Unknown member), the row is never dropped.
    f = key_lookup(f, read_gold(spark, "dim_carrier"), "carrier_code", "carrier_code", "carrier_key", "carrier_key")
    dim_loc = read_gold(spark, "dim_location")
    f = key_lookup(f, dim_loc, "origin_code", "location_code", "location_key", "origin_location_key")
    f = key_lookup(f, dim_loc, "dest_code", "location_code", "location_key", "dest_location_key")
    f = key_lookup(f, read_gold(spark, "dim_service_level"), "service_level", "service_level_code",
                   "service_level_key", "service_level_key")
    f = f.withColumn("_lane_name", F.concat_ws(" -> ", "origin_code", "dest_code"))   # same formula as dim_lane
    f = key_lookup(f, read_gold(spark, "dim_lane"), "_lane_name", "lane_name", "lane_key", "lane_key")
    f = attach_customer_version(f, read_gold(spark, "dim_customer"))                  # SCD2 as-of lookup

    # Measures and flags.
    rate = F.when(F.col("currency") == "USD", F.col("usd_inr_rate")).otherwise(F.lit(1).cast("decimal(10,4)"))
    dec = "decimal(14,2)"
    f = (f.withColumn("fx_rate_applied", rate)
          .withColumn("revenue_freight_inr", to_inr("freight_native"))
          .withColumn("revenue_fuel_inr", to_inr("fuel_native"))
          .withColumn("revenue_detention_inr", to_inr("detention_native"))
          .withColumn("revenue_total_inr", (F.col("revenue_freight_inr") + F.col("revenue_fuel_inr")
                                            + F.col("revenue_detention_inr")).cast(dec))
          .withColumn("quoted_carrier_cost_inr", F.col("quoted_carrier_cost").cast(dec))
          # NULL (not 0) when no invoice has arrived: 'not invoiced yet' is different from 'free'.
          .withColumn("carrier_cost_invoiced_inr", F.col("carrier_cost_invoiced_inr").cast(dec))
          .withColumn("margin_inr", (F.col("revenue_total_inr") - F.col("carrier_cost_invoiced_inr")).cast(dec))
          .withColumn("quote_variance_inr", (F.col("carrier_cost_invoiced_inr") - F.col("quoted_carrier_cost_inr")).cast(dec))
          .withColumn("claim_count", F.coalesce(F.col("claim_count"), F.lit(0)))
          .withColumn("claim_amount_inr", F.coalesce(F.col("claim_amount_inr"), F.lit(0)).cast(dec))
          .withColumn("scan_count", F.coalesce(F.col("scan_count"), F.lit(0)))
          .withColumn("invoice_line_count", F.coalesce(F.col("invoice_line_count"), F.lit(0)))
          .withColumn("is_delivered", F.col("delivered_at").isNotNull())
          # NULL until delivered, so an average over the column only counts delivered shipments.
          .withColumn("is_on_time", F.when(F.col("delivered_at").isNotNull(),
                                           F.col("delivered_at") <= F.col("promised_delivery_at")))
          .withColumn("transit_hours", F.round((F.col("delivered_at").cast("long") - F.col("pickup_at").cast("long")) / 3600, 2))
          .withColumn("delay_hours", F.round((F.col("delivered_at").cast("long") - F.col("promised_delivery_at").cast("long")) / 3600, 2))
          .withColumn("status_delivered_no_scan", (F.col("status") == "DELIVERED") & F.col("delivered_at").isNull())
          # 5% tolerance is a placeholder; the real threshold is a business decision for the metric dictionary.
          .withColumn("is_overbilled", F.when(
              F.col("carrier_cost_invoiced_inr").isNotNull() & F.col("quoted_carrier_cost_inr").isNotNull(),
              F.col("carrier_cost_invoiced_inr") > F.col("quoted_carrier_cost_inr") * F.lit(1.05)))
          .withColumn("dq_warnings", F.concat_ws("|", F.col("_dq_warn_reasons"))))

    return f.select(
        sk(F.col("shipment_ref")).alias("shipment_key"), "shipment_ref", "shipment_id",
        "customer_key", "carrier_key", "origin_location_key", "dest_location_key", "lane_key", "service_level_key",
        date_key("booked_at").alias("booked_date_key"), date_key("promised_delivery_at").alias("promised_date_key"),
        date_key("pickup_at").alias("pickup_date_key"), date_key("delivered_at").alias("delivered_date_key"),
        "booked_at", "promised_delivery_at", "pickup_at", "delivered_at", "status",
        F.col("currency").alias("billing_currency"), "fx_rate_applied", "weight_kg", "volume_cbm",
        "revenue_freight_inr", "revenue_fuel_inr", "revenue_detention_inr", "revenue_total_inr",
        "quoted_carrier_cost_inr", "carrier_cost_invoiced_inr", "margin_inr", "quote_variance_inr",
        "claim_count", "claim_amount_inr", "scan_count", "invoice_line_count", "transit_hours", "delay_hours",
        "is_delivered", "is_on_time", "is_overbilled", "status_delivered_no_scan", "dq_warnings",
        F.col("updated_at").alias("source_updated_at"),          # the watermark column for Stage 10
        F.lit(batch).alias("_gold_batch_id"), F.current_timestamp().alias("_gold_processed_at"))


# ---------------------------------------------------------------- fact_tracking_event
def build_fact_tracking(spark, batch: str, refs: DataFrame = None) -> DataFrame:
    scans = read_silver(spark, "scans")
    if refs is not None:
        # The window below needs a shipment's COMPLETE timeline, so take ALL scans of the affected
        # shipments, not just the new ones.
        scans = scans.join(F.broadcast(refs.select("shipment_ref")), "shipment_ref", "left_semi")
    # Window function: order each shipment's scans by time and look at the previous one.
    # partitionBy(shipment_ref) makes Spark shuffle all scans of a shipment into the same partition.
    w = Window.partitionBy("shipment_ref").orderBy("event_ts")
    f = scans.withColumn("hours_since_prev_scan", F.round(
        (F.col("event_ts").cast("long") - F.lag("event_ts").over(w).cast("long")) / 3600, 2))
    f = key_lookup(f, read_gold(spark, "dim_location"), "location_code", "location_code", "location_key", "location_key")
    return f.select(
        sk(F.col("shipment_ref"), F.col("event_type"), F.col("event_ts"), F.col("location_code")).alias("event_key"),
        "shipment_ref", sk(F.col("shipment_ref")).alias("shipment_key"),   # same hash as fact_shipment: no lookup needed
        date_key("event_date").alias("event_date_key"), "location_key", "event_type", "event_ts",
        date_key("arrival_date").alias("arrival_date_key"), "arrival_lag_days",
        (F.col("arrival_lag_days") > 0).alias("is_late_arrival"), "scan_source", "hours_since_prev_scan",
        F.concat_ws("|", F.col("_dq_warn_reasons")).alias("dq_warnings"),
        F.lit(batch).alias("_gold_batch_id"), F.current_timestamp().alias("_gold_processed_at"))


# ---------------------------------------------------------------- fact_carrier_invoice_line
def build_fact_invoice(spark, batch: str, since: str = None) -> DataFrame:
    lines = read_silver(spark, "carrier_invoice_lines")
    if since:   # incremental: only lines Silver processed after the Gold watermark
        lines = lines.filter(F.col("_silver_processed_at") > F.lit(since).cast("timestamp"))
    inv = key_lookup(lines, read_gold(spark, "dim_carrier"),
                     "carrier_code", "carrier_code", "carrier_key", "carrier_key")
    return inv.select(
        sk(F.col("carrier_code"), F.col("invoice_no"), F.col("shipment_ref"), F.col("charge_type")).alias("invoice_line_key"),
        "carrier_key", date_key("invoice_date").alias("invoice_date_key"),
        "shipment_ref", sk(F.col("shipment_ref")).alias("shipment_key"), "invoice_no", "charge_type",
        F.col("amount").cast("decimal(14,2)").alias("amount_inr"),
        F.col("gst_amount").cast("decimal(14,2)").alias("gst_amount_inr"), "currency",
        F.concat_ws("|", F.col("_dq_warn_reasons")).alias("dq_warnings"),
        F.lit(batch).alias("_gold_batch_id"), F.current_timestamp().alias("_gold_processed_at"))


# ---------------------------------------------------------------- write + grain test
def publish(spark, batch: str, name: str, key: str, df: DataFrame, expected_rows: int) -> None:
    out = gold_path(name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)
    written = spark.read.format("delta").load(out)
    n, k = written.count(), written.select(key).distinct().count()
    ok = n == k == expected_rows          # completeness (= Silver) AND grain (one row per key)
    log_gold(spark, batch, name, expected_rows, n)
    print(f"gold {name:<26} rows={n:>10,}  silver rows={expected_rows:>10,}  distinct keys={k:>10,}  "
          f"{'PASS' if ok else 'FAIL'}")
    if not ok:
        raise RuntimeError(f"{name}: grain or completeness check failed")


def main():
    spark = get_spark("gold_facts")
    batch = new_batch_id("run")
    publish(spark, batch, "fact_shipment", "shipment_key", build_fact_shipment(spark, batch),
            read_silver(spark, "shipments").count())
    publish(spark, batch, "fact_tracking_event", "event_key", build_fact_tracking(spark, batch),
            read_silver(spark, "scans").count())
    publish(spark, batch, "fact_carrier_invoice_line", "invoice_line_key", build_fact_invoice(spark, batch),
            read_silver(spark, "carrier_invoice_lines").count())
    spark.stop()


if __name__ == "__main__":
    main()
