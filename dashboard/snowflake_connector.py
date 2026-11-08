"""
snowflake_connector.py — data access for the dashboard.

Standalone mode (default): when SNOWFLAKE_ACCOUNT is not set, every
getter serves the deterministic demo dataset from ``demo_data`` —
generated from the project's own star-schema DDL and the PySpark
enrichment logic in ``processing/jobs/enrich.py`` — so the console runs
anywhere with no credentials.

Live mode: when SNOWFLAKE_ACCOUNT (plus SNOWFLAKE_USER /
SNOWFLAKE_PASSWORD) is set, the getters query the Snowflake views
``merchant_daily_performance_v``, ``fraud_velocity_alerts_v`` and
``customer_spend_profile_v`` exactly as before.
"""

import os
from datetime import datetime, timedelta
import random

import pandas as pd

USE_SYNTHETIC = not bool(os.environ.get("SNOWFLAKE_ACCOUNT"))


def get_connection():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "CONNECTED_COMMERCE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )


def query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def _demo():
    try:
        from dashboard.demo_data import build_all
    except ModuleNotFoundError:
        from demo_data import build_all  # type: ignore
    return build_all()


def get_merchant_performance() -> pd.DataFrame:
    if USE_SYNTHETIC:
        return _demo()["merchant_daily"]
    return query("SELECT * FROM connected_commerce.merchant_daily_performance_v ORDER BY txn_date DESC LIMIT 5000")


def get_fraud_alerts() -> pd.DataFrame:
    if USE_SYNTHETIC:
        return _demo()["fraud_alerts"]
    return query("SELECT * FROM connected_commerce.fraud_velocity_alerts_v ORDER BY event_time DESC LIMIT 5000")


def get_customer_profiles() -> pd.DataFrame:
    if USE_SYNTHETIC:
        return _demo()["customers"]
    return query("SELECT * FROM connected_commerce.customer_spend_profile_v LIMIT 500")


def get_pipeline_health() -> dict:
    if USE_SYNTHETIC:
        pipe = _demo()["pipeline_daily"]
        last = pipe.iloc[-1]
        return {
            "last_kafka_flush": datetime.now() - timedelta(minutes=3),
            "records_last_batch": int(last["records_loaded"]) // 32,
            "quarantine_rate_pct": float(last["quarantine_rate_pct"]),
            "snowflake_load_latency": float(last["load_latency_s"]),
            "total_loaded_today": int(last["records_loaded"]),
            "dlq_count_today": int(last["dlq_count"]),
            "schema_violations_today": int(last["schema_violations"]),
        }
    row = query("""
        SELECT
            MAX(dw_loaded_at)                          AS last_load,
            COUNT(*)                                   AS total_loaded_today,
            ROUND(AVG(DATEDIFF('second', event_time, dw_loaded_at)), 1) AS avg_latency_s
        FROM connected_commerce.fact_transactions
        WHERE ingest_date = CURRENT_DATE()
    """).iloc[0]
    return {
        "last_kafka_flush": row["last_load"],
        "total_loaded_today": int(row["total_loaded_today"]),
        "snowflake_load_latency": float(row["avg_latency_s"]),
        "quarantine_rate_pct": 0.0,
        "dlq_count_today": 0,
        "schema_violations_today": 0,
        "records_last_batch": 0,
    }
