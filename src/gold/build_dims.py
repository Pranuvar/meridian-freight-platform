"""Gold: date, location, carrier, service level and lane dimensions.
These are rebuilt in full each run (they are tiny). Keys are deterministic hashes, so a rebuild
never changes a key that facts already reference."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.bronze.common import new_batch_id
from src.gold.common import UNKNOWN_KEY, gold_path, log_gold, sk
from src.silver.common import silver_path
from src.utils.spark import get_spark

# Reference data owned by the business (not in any source system): code, name, base transit days, sort order.
SERVICE_LEVELS = [("EXPRESS", "Express", 2, 1), ("STANDARD", "Standard", 4, 2), ("ECONOMY", "Economy", 7, 3)]


def build_dim_date(spark, start="2024-01-01", end="2027-12-31") -> DataFrame:
    days = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
    d, m, y = F.col("date"), F.month("date"), F.year("date")
    # Spark's dayofweek() is 1=Sunday..7=Saturday. This converts to ISO: 1=Monday..7=Sunday.
    iso_dow = ((F.dayofweek(d) + 5) % 7) + 1
    # India's financial year runs April to March: April 2026 is in FY2026-27, March 2026 in FY2025-26.
    fy_start = F.when(m >= 4, y).otherwise(y - 1)
    return days.select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),   # readable key, e.g. 20260401
        d.alias("date"),
        y.alias("year"),
        F.quarter(d).alias("quarter"),
        m.alias("month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.date_format(d, "yyyy-MM").alias("year_month"),      # sorts correctly as text, handy for Power BI
        F.trunc(d, "month").alias("month_start_date"),
        F.weekofyear(d).alias("iso_week"),
        iso_dow.alias("day_of_week"),
        F.date_format(d, "EEEE").alias("day_name"),
        (iso_dow >= 6).alias("is_weekend"),
        F.concat(F.lit("FY"), fy_start.cast("string"), F.lit("-"),
                 F.lpad(((fy_start + 1) % 100).cast("string"), 2, "0")).alias("fiscal_year"),
        (F.floor(((m - 4 + 12) % 12) / 3) + 1).cast("int").alias("fiscal_quarter"),
    )


def build_dim_location(spark) -> DataFrame:
    src = spark.read.format("delta").load(silver_path("locations"))
    dim = src.select(sk(F.col("location_code")).alias("location_key"), "location_code",
                     F.col("name").alias("location_name"), "city", "state", "region", "location_type")
    unknown = spark.createDataFrame(
        [(UNKNOWN_KEY, "UNKNOWN", "Unknown location", "Unknown", "Unknown", "Unknown", "UNKNOWN")],
        schema=dim.schema)
    return dim.unionByName(unknown)


def build_dim_carrier(spark) -> DataFrame:
    src = spark.read.format("delta").load(silver_path("carriers"))
    dim = src.select(sk(F.col("carrier_code")).alias("carrier_key"), "carrier_code", "carrier_name",
                     F.col("mode").alias("transport_mode"), "is_active")
    unknown = spark.createDataFrame([(UNKNOWN_KEY, "UNKNOWN", "Unknown carrier", "UNKNOWN", False)],
                                    schema=dim.schema)
    return dim.unionByName(unknown)


def build_dim_service_level(spark) -> DataFrame:
    base = spark.createDataFrame(
        SERVICE_LEVELS, "service_level_code string, service_level_name string, "
                        "base_transit_days int, sort_order int")
    dim = base.select(sk(F.col("service_level_code")).alias("service_level_key"), "*")
    unknown = spark.createDataFrame([(UNKNOWN_KEY, "UNKNOWN", "Unknown", None, 99)], schema=dim.schema)
    return dim.unionByName(unknown)


def build_dim_lane(spark, dim_location: DataFrame) -> DataFrame:
    """Every possible lane = every hub (origin) x every location (destination), minus same-place pairs.
    Built from reference data, not from shipments, so a lane exists before its first shipment."""
    loc = dim_location.filter(F.col("location_key") != UNKNOWN_KEY)
    origins = loc.filter("location_type = 'HUB'").select(
        F.col("location_key").alias("origin_location_key"), F.col("location_code").alias("origin_code"),
        F.col("region").alias("origin_region"))
    dests = loc.select(
        F.col("location_key").alias("dest_location_key"), F.col("location_code").alias("dest_code"),
        F.col("region").alias("dest_region"))
    lanes = origins.crossJoin(dests).filter("origin_code <> dest_code")
    dim = lanes.select(
        sk(F.col("origin_code"), F.col("dest_code")).alias("lane_key"),
        F.concat_ws(" -> ", "origin_code", "dest_code").alias("lane_name"),
        "origin_location_key", "origin_code", "origin_region",
        "dest_location_key", "dest_code", "dest_region",
        (F.col("origin_region") != F.col("dest_region")).alias("is_inter_region"))
    unknown = spark.createDataFrame(
        [(UNKNOWN_KEY, "Unknown lane", UNKNOWN_KEY, "UNKNOWN", "Unknown", UNKNOWN_KEY, "UNKNOWN", "Unknown", False)],
        schema=dim.schema)
    return dim.unionByName(unknown)


def write_dim(spark, batch, name, key, df):
    out = gold_path(name)
    df.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out)   # tiny table: one file
    written = spark.read.format("delta").load(out)
    n_out, n_keys = written.count(), written.select(key).distinct().count()
    if n_keys != n_out:      # a duplicate natural key or a hash collision: stop, never publish it
        raise RuntimeError(f"{name}: {n_out - n_keys} duplicate surrogate keys")
    log_gold(spark, batch, name, n_out, n_out)
    print(f"gold {name:<20} rows={n_out:>6,}  keys unique: PASS")


def main():
    spark = get_spark("gold_dims")
    batch = new_batch_id("run")
    location = build_dim_location(spark)
    write_dim(spark, batch, "dim_date", "date_key", build_dim_date(spark))
    write_dim(spark, batch, "dim_location", "location_key", location)
    write_dim(spark, batch, "dim_carrier", "carrier_key", build_dim_carrier(spark))
    write_dim(spark, batch, "dim_service_level", "service_level_key", build_dim_service_level(spark))
    write_dim(spark, batch, "dim_lane", "lane_key", build_dim_lane(spark, location))
    spark.stop()


if __name__ == "__main__":
    main()
