"""
validate.py — PySpark schema enforcement and data quality gate.

Reads raw AVRO files from S3 landing zone, applies:
  1. Schema casting and type enforcement
  2. Null checks on critical fields
  3. Business rule validation (amount bounds, enum membership, timestamp format)
  4. Quarantine: invalid rows → S3 quarantine prefix as JSON; valid rows passed forward

Output: cleaned DataFrame returned to caller (used by enrich.py in the pipeline)

Usage (standalone):
    spark-submit processing/jobs/validate.py --input-path s3://bucket/raw/transactions/
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running as a script: `python processing/jobs/validate.py ...`
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField, StructType, TimestampType,
)

from processing.utils.spark_session import get_spark

log = logging.getLogger("validate")

# ── Expected schema ────────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),  nullable=False),
    StructField("customer_id",       StringType(),  nullable=False),
    StructField("merchant_id",       StringType(),  nullable=False),
    StructField("merchant_name",     StringType(),  nullable=True),
    StructField("merchant_category", StringType(),  nullable=False),
    StructField("city",              StringType(),  nullable=True),
    StructField("state",             StringType(),  nullable=True),
    StructField("amount_usd",        DoubleType(),  nullable=False),
    StructField("currency",          StringType(),  nullable=True),
    StructField("account_type",      StringType(),  nullable=False),
    StructField("transaction_type",  StringType(),  nullable=False),
    StructField("channel",           StringType(),  nullable=False),
    StructField("event_time",        StringType(),  nullable=False),
    StructField("kafka_ingest_time", StringType(),  nullable=True),
    StructField("is_international",  BooleanType(), nullable=True),
    StructField("zip_code",          StringType(),  nullable=True),
])

VALID_ACCOUNT_TYPES   = {"checking", "credit", "debit"}
VALID_TXN_TYPES       = {"purchase", "refund", "chargeback", "atm_withdrawal"}
VALID_CHANNELS        = {"in_store", "online", "mobile", "contactless"}
VALID_CATEGORIES      = {
    "grocery", "restaurant", "gas_station", "pharmacy", "retail_clothing",
    "electronics", "travel_hotel", "travel_airline", "entertainment",
    "healthcare", "subscription_services", "atm",
}


def load_raw(spark: SparkSession, input_path: str) -> DataFrame:
    """Read AVRO files from S3 raw landing zone."""
    log.info("Reading raw AVRO from: %s", input_path)
    return spark.read.format("avro").load(input_path)


def enforce_schema(df: DataFrame) -> DataFrame:
    """Cast all columns to expected types and parse event_time to timestamp."""
    return (
        df
        .withColumn("amount_usd",       F.col("amount_usd").cast(DoubleType()))
        .withColumn("is_international", F.col("is_international").cast(BooleanType()))
        .withColumn("event_time_ts",    F.to_timestamp("event_time"))
        .withColumn("ingest_date",      F.to_date("event_time"))
    )


def build_validation_flags(df: DataFrame) -> DataFrame:
    """
    Add boolean flag columns for each validation rule.
    Keeping flags separate makes it easy to audit which rule failed per row.
    """
    now = F.lit(datetime.now(timezone.utc).isoformat()).cast(TimestampType())

    return (
        df
        # Null checks
        .withColumn("_chk_txn_id_notnull",    F.col("transaction_id").isNotNull())
        .withColumn("_chk_customer_notnull",   F.col("customer_id").isNotNull())
        .withColumn("_chk_merchant_notnull",   F.col("merchant_id").isNotNull())
        .withColumn("_chk_amount_notnull",     F.col("amount_usd").isNotNull())
        .withColumn("_chk_event_time_notnull", F.col("event_time").isNotNull())
        # Amount bounds: $0.01 – $50,000
        .withColumn("_chk_amount_range",
            (F.col("amount_usd") >= 0.01) & (F.col("amount_usd") <= 50_000.00)
        )
        # Enum membership
        .withColumn("_chk_account_type",
            F.col("account_type").isin(list(VALID_ACCOUNT_TYPES))
        )
        .withColumn("_chk_txn_type",
            F.col("transaction_type").isin(list(VALID_TXN_TYPES))
        )
        .withColumn("_chk_channel",
            F.col("channel").isin(list(VALID_CHANNELS))
        )
        .withColumn("_chk_category",
            F.col("merchant_category").isin(list(VALID_CATEGORIES))
        )
        # Timestamp not null and not in the future
        .withColumn("_chk_event_time_valid",
            F.col("event_time_ts").isNotNull() & (F.col("event_time_ts") <= now)
        )
        # transaction_id looks like a UUID (basic format check)
        .withColumn("_chk_txn_id_format",
            F.col("transaction_id").rlike(
                r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
            )
        )
    )


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split into valid and quarantine DataFrames based on all _chk_ columns.
    A row is valid only if ALL checks pass.
    """
    check_cols = [c for c in df.columns if c.startswith("_chk_")]

    # All checks must be True
    all_valid_condition = F.reduce(
        lambda a, b: a & b,
        [F.col(c) for c in check_cols]
    )

    df = df.withColumn("_is_valid", all_valid_condition)

    valid_df = (
        df.filter(F.col("_is_valid"))
          .drop(*check_cols, "_is_valid")
    )

    invalid_df = (
        df.filter(~F.col("_is_valid"))
          .withColumn("_quarantine_ts", F.current_timestamp())
          # Collect which rules failed
          .withColumn("_failed_checks",
              F.concat_ws(",", *[
                  F.when(~F.col(c), F.lit(c)).otherwise(F.lit(None))
                  for c in check_cols
              ])
          )
    )

    return valid_df, invalid_df


def write_quarantine(invalid_df: DataFrame, quarantine_path: str) -> int:
    """Write invalid rows to quarantine as JSON for manual inspection."""
    count = invalid_df.count()
    if count > 0:
        log.warning("Quarantining %d invalid rows → %s", count, quarantine_path)
        (
            invalid_df
            .write
            .mode("append")
            .partitionBy("ingest_date")
            .json(quarantine_path)
        )
    return count


def run_validation(
    spark: SparkSession,
    input_path: str,
    quarantine_path: str,
) -> DataFrame:
    """
    Full validation pipeline. Returns the valid DataFrame for downstream use.
    """
    raw_df     = load_raw(spark, input_path)
    typed_df   = enforce_schema(raw_df)
    flagged_df = build_validation_flags(typed_df)

    valid_df, invalid_df = split_valid_invalid(flagged_df)

    total    = raw_df.count()
    quarantined = write_quarantine(invalid_df, quarantine_path)
    passed   = total - quarantined

    log.info("Validation complete | total=%d | passed=%d | quarantined=%d", total, passed, quarantined)

    if passed == 0:
        raise ValueError("All records failed validation — aborting pipeline.")

    return valid_df


# ── CLI entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path",      required=True)
    parser.add_argument("--quarantine-path", default="s3://connected-commerce-lake/quarantine/")
    args = parser.parse_args()

    spark = get_spark("CC-Validate")
    df    = run_validation(spark, args.input_path, args.quarantine_path)
    df.printSchema()
    print(f"Valid record count: {df.count()}")