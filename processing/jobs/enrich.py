"""
enrich.py — PySpark enrichment job: velocity features + merchant lookup join.

Takes the validated DataFrame and adds:
  1. txn_count_last_1h      — number of transactions by this customer in last 1 hour
  2. spend_velocity_24h     — total spend by this customer in last 24 hours
  3. cross_merchant_flag    — True if customer used card at 5+ distinct merchants in 1 hour
  4. risk_tier              — derived from spend_velocity_24h thresholds
  5. hour_of_day / day_of_week — time-based features for downstream analytics

Window functions operate on event_time_ts using rangeBetween over Unix timestamps
to avoid a full sort per partition.
"""

import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

log = logging.getLogger("enrich")

# ── Risk tier thresholds (USD) ─────────────────────────────────────────────────
RISK_TIER_HIGH   = 5_000.0
RISK_TIER_MEDIUM = 1_000.0


def add_time_features(df: DataFrame) -> DataFrame:
    """Extract hour_of_day and day_of_week from event_time_ts."""
    return (
        df
        .withColumn("hour_of_day",  F.hour("event_time_ts"))
        .withColumn("day_of_week",  F.dayofweek("event_time_ts"))   # 1=Sunday … 7=Saturday
        .withColumn("week_of_year", F.weekofyear("event_time_ts"))
    )


def add_velocity_features(df: DataFrame) -> DataFrame:
    """
    Compute rolling window aggregations per customer over event_time_ts.

    Uses rangeBetween on Unix epoch seconds so the window is a true
    time-based range, not a row-count range.

    Windows:
      - 1-hour lookback  → txn_count_last_1h, distinct_merchants_1h
      - 24-hour lookback → spend_velocity_24h
    """
    # Convert event_time to Unix seconds for rangeBetween
    df = df.withColumn("event_epoch", F.unix_timestamp("event_time_ts"))

    ONE_HOUR  = 3600
    ONE_DAY   = 86_400

    w_1h = (
        Window
        .partitionBy("customer_id")
        .orderBy("event_epoch")
        .rangeBetween(-ONE_HOUR, 0)
    )

    w_24h = (
        Window
        .partitionBy("customer_id")
        .orderBy("event_epoch")
        .rangeBetween(-ONE_DAY, 0)
    )

    return (
        df
        # Transaction count in last 1 hour per customer
        .withColumn("txn_count_last_1h",
            F.count("transaction_id").over(w_1h)
        )
        # Distinct merchants visited in last 1 hour per customer
        .withColumn("distinct_merchants_1h",
            F.approx_count_distinct("merchant_id").over(w_1h)
        )
        # Total spend in last 24 hours per customer
        .withColumn("spend_velocity_24h",
            F.sum("amount_usd").over(w_24h)
        )
        # Cross-merchant flag: card used at 5+ distinct merchants in 1 hour
        .withColumn("cross_merchant_flag",
            F.col("distinct_merchants_1h") >= 5
        )
    )


def add_risk_tier(df: DataFrame) -> DataFrame:
    """
    Derive risk_tier from spend_velocity_24h.
      high   : >= $5,000 in 24h
      medium : >= $1,000 in 24h
      low    : < $1,000 in 24h
    """
    return df.withColumn(
        "risk_tier",
        F.when(F.col("spend_velocity_24h") >= RISK_TIER_HIGH,   F.lit("high"))
         .when(F.col("spend_velocity_24h") >= RISK_TIER_MEDIUM, F.lit("medium"))
         .otherwise(F.lit("low"))
    )


def add_merchant_segment(df: DataFrame) -> DataFrame:
    """
    Map merchant_category to a higher-level business segment.
    This simulates a lookup join — in production this would be a
    broadcast join against a dim_merchant_category reference table.
    """
    segment_map = {
        "grocery":               "everyday_spend",
        "restaurant":            "everyday_spend",
        "gas_station":           "everyday_spend",
        "pharmacy":              "healthcare_wellness",
        "healthcare":            "healthcare_wellness",
        "retail_clothing":       "discretionary",
        "electronics":           "discretionary",
        "entertainment":         "discretionary",
        "travel_hotel":          "travel",
        "travel_airline":        "travel",
        "subscription_services": "recurring",
        "atm":                   "cash_access",
    }

    # Build a MapType literal for the lookup
    map_expr = F.create_map([F.lit(x) for pair in segment_map.items() for x in pair])

    return df.withColumn(
        "merchant_segment",
        F.coalesce(map_expr[F.col("merchant_category")], F.lit("other"))
    )


def enrich(df: DataFrame) -> DataFrame:
    """
    Full enrichment pipeline. Apply all feature engineering steps in order.
    Returns the enriched DataFrame ready for Iceberg write.
    """
    log.info("Starting enrichment pipeline...")

    df = add_time_features(df)
    df = add_velocity_features(df)
    df = add_risk_tier(df)
    df = add_merchant_segment(df)

    # Drop intermediate column
    df = df.drop("event_epoch")

    log.info("Enrichment complete. Schema:")
    df.printSchema()

    return df