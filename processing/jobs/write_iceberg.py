"""
write_iceberg.py — Write enriched DataFrame to Iceberg table on S3.

Creates the Iceberg table if it does not exist, then appends the batch.
Partitioned by: year, month, day (derived from ingest_date).

Table: glue_catalog.connected_commerce.transactions_curated

Usage (standalone):
    spark-submit processing/jobs/write_iceberg.py \
        --input-path s3://bucket/raw/transactions/ \
        --output-table glue_catalog.connected_commerce.transactions_curated
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running as a script: `python processing/jobs/write_iceberg.py ...`
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from processing.utils.spark_session import get_spark
from processing.jobs.validate import run_validation
from processing.jobs.enrich import enrich

log = logging.getLogger("write_iceberg")

DEFAULT_TABLE      = "glue_catalog.connected_commerce.transactions_curated"
DEFAULT_QUARANTINE = "s3://connected-commerce-lake/quarantine/"


def create_table_if_not_exists(spark: SparkSession, table: str) -> None:
    """
    Create the Iceberg table with the curated schema if it doesn't exist.
    Partitioned by year/month/day derived from ingest_date.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            transaction_id       STRING     NOT NULL,
            customer_id          STRING     NOT NULL,
            merchant_id          STRING     NOT NULL,
            merchant_name        STRING,
            merchant_category    STRING,
            merchant_segment     STRING,
            city                 STRING,
            state                STRING,
            amount_usd           DOUBLE,
            currency             STRING,
            account_type         STRING,
            transaction_type     STRING,
            channel              STRING,
            is_international     BOOLEAN,
            zip_code             STRING,
            event_time           STRING,
            event_time_ts        TIMESTAMP,
            ingest_date          DATE,
            hour_of_day          INT,
            day_of_week          INT,
            week_of_year         INT,
            txn_count_last_1h    LONG,
            distinct_merchants_1h LONG,
            spend_velocity_24h   DOUBLE,
            cross_merchant_flag  BOOLEAN,
            risk_tier            STRING
        )
        USING iceberg
        PARTITIONED BY (year(ingest_date), month(ingest_date), day(ingest_date))
        TBLPROPERTIES (
            'write.format.default'          = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.metadata.compression-codec' = 'gzip',
            'history.expire.max-snapshot-age-ms' = '604800000'
        )
    """)
    log.info("Iceberg table ready: %s", table)


def write(df: DataFrame, table: str) -> None:
    """Append enriched batch to the Iceberg table."""
    row_count = df.count()
    log.info("Writing %d rows to Iceberg table: %s", row_count, table)

    (
        df.writeTo(table)
          .option("check-ordering", "false")
          .append()
    )

    log.info("Write complete.")


def run_pipeline(
    spark: SparkSession,
    input_path: str,
    output_table: str,
    quarantine_path: str,
) -> None:
    """End-to-end: validate → enrich → write Iceberg."""
    # Step 1: Validate
    valid_df = run_validation(spark, input_path, quarantine_path)

    # Step 2: Enrich
    enriched_df = enrich(valid_df)

    # Step 3: Create table + write
    create_table_if_not_exists(spark, output_table)
    write(enriched_df, output_table)

    log.info("Pipeline complete: %s → %s", input_path, output_table)


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path",      required=True, help="S3 path to raw AVRO files")
    parser.add_argument("--output-table",    default=DEFAULT_TABLE)
    parser.add_argument("--quarantine-path", default=DEFAULT_QUARANTINE)
    args = parser.parse_args()

    spark = get_spark("CC-ETL")
    run_pipeline(spark, args.input_path, args.output_table, args.quarantine_path)