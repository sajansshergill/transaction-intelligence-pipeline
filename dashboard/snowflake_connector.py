"""
snowflake_connector.py — Snowflake connection + query helpers for the dashboard.

Falls back to synthetic data if SNOWFLAKE_ACCOUNT is not set,
so the dashboard runs locally without any credentials.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ── Connection ─────────────────────────────────────────────────────────────────
def get_connection():
    import snowflake.connector
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = os.environ.get("SNOWFLAKE_DATABASE",  "CONNECTED_COMMERCE"),
        schema    = os.environ.get("SNOWFLAKE_SCHEMA",    "PUBLIC"),
    )


def query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


# ── Synthetic fallback data ────────────────────────────────────────────────────
CATEGORIES = [
    "grocery", "restaurant", "gas_station", "pharmacy",
    "retail_clothing", "electronics", "travel_hotel",
    "entertainment", "healthcare", "subscription_services",
]
SEGMENTS   = ["everyday_spend", "discretionary", "travel", "healthcare_wellness", "recurring"]
STATES     = ["NY", "CA", "TX", "FL", "IL", "WA", "GA", "MA", "NJ", "PA"]
CHANNELS   = ["in_store", "online", "mobile", "contactless"]


def _random_dates(n: int, days_back: int = 30) -> list:
    base = datetime.now()
    return [base - timedelta(days=random.randint(0, days_back),
                             hours=random.randint(0, 23)) for _ in range(n)]


def synthetic_merchant_performance(n_rows: int = 300) -> pd.DataFrame:
    random.seed(42)
    np.random.seed(42)
    merchants = [f"Merchant_{i:03d}" for i in range(1, 31)]
    rows = []
    for _ in range(n_rows):
        m   = random.choice(merchants)
        cat = random.choice(CATEGORIES)
        rev = round(np.random.lognormal(6.5, 1.2), 2)
        rows.append({
            "txn_date":           (datetime.now() - timedelta(days=random.randint(0, 29))).date(),
            "merchant_name":      m,
            "merchant_category":  cat,
            "merchant_segment":   random.choice(SEGMENTS),
            "state":              random.choice(STATES),
            "txn_count":          random.randint(5, 400),
            "total_revenue_usd":  rev,
            "avg_ticket_usd":     round(rev / random.randint(5, 100), 2),
            "revenue_rolling_7d": round(rev * random.uniform(5, 9), 2),
            "revenue_dod_delta":  round(np.random.normal(0, 500), 2),
        })
    return pd.DataFrame(rows)


def synthetic_fraud_alerts(n_rows: int = 150) -> pd.DataFrame:
    random.seed(7)
    rows = []
    for i in range(n_rows):
        score = random.choices([1, 2, 3, 4], weights=[40, 30, 20, 10])[0]
        rows.append({
            "transaction_id":      f"txn-{i:06d}",
            "event_time":          _random_dates(1)[0],
            "customer_id":         f"CUST_{random.randint(1000,9999)}",
            "merchant_name":       f"Merchant_{random.randint(1,30):03d}",
            "merchant_category":   random.choice(CATEGORIES),
            "state":               random.choice(STATES),
            "amount_usd":          round(random.uniform(0.5, 4999), 2),
            "txn_count_last_1h":   random.randint(1, 25),
            "distinct_merchants_1h": random.randint(1, 8),
            "spend_velocity_24h":  round(random.uniform(100, 15000), 2),
            "channel":             random.choice(CHANNELS),
            "cross_merchant_flag": score >= 3,
            "is_international":    random.random() < 0.35,
            "risk_tier":           random.choices(["low","medium","high"], weights=[50,30,20])[0],
            "high_velocity_flag":  random.random() < 0.3,
            "signal_label":        random.choice(["high_cross_merchant","medium_intl","high_velocity","low"]),
            "alert_score":         score,
        })
    return pd.DataFrame(rows)


def synthetic_pipeline_health() -> dict:
    return {
        "last_kafka_flush":       datetime.now() - timedelta(minutes=random.randint(1, 8)),
        "records_last_batch":     random.randint(800, 3500),
        "quarantine_rate_pct":    round(random.uniform(0.1, 2.5), 2),
        "snowflake_load_latency": round(random.uniform(4.2, 18.7), 1),
        "total_loaded_today":     random.randint(40_000, 120_000),
        "dlq_count_today":        random.randint(0, 12),
        "schema_violations_today": random.randint(0, 5),
    }


def synthetic_customer_profile(n_rows: int = 200) -> pd.DataFrame:
    random.seed(13)
    rows = []
    for i in range(n_rows):
        spend = round(np.random.lognormal(8, 1.5), 2)
        rows.append({
            "customer_id":                f"CUST_{i:04d}",
            "account_type":               random.choice(["checking","credit","debit"]),
            "risk_tier":                  random.choices(["low","medium","high"], weights=[60,30,10])[0],
            "lifetime_txn_count":         random.randint(1, 500),
            "lifetime_spend_usd":         spend,
            "avg_txn_usd":                round(spend / random.randint(1,500), 2),
            "recency_days":               random.randint(0, 90),
            "txn_frequency_per_day":      round(random.uniform(0.1, 3.5), 2),
            "distinct_merchants_visited": random.randint(1, 40),
            "spend_segment":              random.choices(["bronze","silver","gold","platinum"],
                                                         weights=[50,30,15,5])[0],
            "cross_merchant_events":      random.randint(0, 20),
            "intl_txn_count":             random.randint(0, 10),
        })
    return pd.DataFrame(rows)


# ── Public API ─────────────────────────────────────────────────────────────────
USE_SYNTHETIC = not bool(os.environ.get("SNOWFLAKE_ACCOUNT"))


def get_merchant_performance() -> pd.DataFrame:
    if USE_SYNTHETIC:
        return synthetic_merchant_performance()
    return query("SELECT * FROM connected_commerce.merchant_daily_performance_v ORDER BY txn_date DESC LIMIT 500")


def get_fraud_alerts() -> pd.DataFrame:
    if USE_SYNTHETIC:
        return synthetic_fraud_alerts()
    return query("SELECT * FROM connected_commerce.fraud_velocity_alerts_v ORDER BY event_time DESC LIMIT 500")


def get_pipeline_health() -> dict:
    if USE_SYNTHETIC:
        return synthetic_pipeline_health()
    row = query("""
        SELECT
            MAX(dw_loaded_at)                          AS last_load,
            COUNT(*)                                   AS total_loaded_today,
            ROUND(AVG(DATEDIFF('second', event_time, dw_loaded_at)), 1) AS avg_latency_s
        FROM connected_commerce.fact_transactions
        WHERE ingest_date = CURRENT_DATE()
    """).iloc[0]
    return {
        "last_kafka_flush":       row["last_load"],
        "total_loaded_today":     int(row["total_loaded_today"]),
        "snowflake_load_latency": float(row["avg_latency_s"]),
        "quarantine_rate_pct":    0.0,
        "dlq_count_today":        0,
        "schema_violations_today": 0,
        "records_last_batch":     0,
    }


def get_customer_profiles() -> pd.DataFrame:
    if USE_SYNTHETIC:
        return synthetic_customer_profile()
    return query("SELECT * FROM connected_commerce.customer_spend_profile_v LIMIT 500")