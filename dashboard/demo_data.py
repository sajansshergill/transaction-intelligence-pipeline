"""Demo-data engine for the Transaction Intelligence console.

Generates deterministic, domain-faithful data derived directly from the
project's own schemas and pipeline logic — no Snowflake or external
credentials required:

* Merchants / categories / segments follow ``dim_merchant`` and the
  category -> segment map in ``processing/jobs/enrich.py``
  (``add_merchant_segment``).
* Transactions follow ``fact_transactions`` (amount_usd, channel,
  txn_count_last_1h, distinct_merchants_1h, spend_velocity_24h).
* Velocity features and risk tiers replicate ``enrich.py`` exactly:
  ``high`` when spend_velocity_24h >= $5,000, ``medium`` >= $1,000,
  ``high_velocity_flag`` when txn_count_last_1h > 10 and
  ``cross_merchant_flag`` when distinct_merchants_1h >= 5.
* Merchant daily rows mirror ``merchant_daily_performance_v``,
  fraud rows mirror ``fraud_velocity_alerts_v`` and customer rows
  mirror ``customer_spend_profile_v`` (platinum >= $50k lifetime,
  gold >= $10k, silver >= $1k).

When SNOWFLAKE_ACCOUNT is configured the dashboard can still query the
live Snowflake views via ``snowflake_connector``; this module is the
standalone mode wired from those same schemas.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Category -> segment map, copied from processing/jobs/enrich.py
SEGMENT_MAP = {
    "grocery": "everyday_spend",
    "restaurant": "everyday_spend",
    "gas_station": "everyday_spend",
    "pharmacy": "healthcare_wellness",
    "healthcare": "healthcare_wellness",
    "retail_clothing": "discretionary",
    "electronics": "discretionary",
    "entertainment": "discretionary",
    "travel_hotel": "travel",
    "travel_airline": "travel",
    "subscription_services": "recurring",
    "atm": "cash_access",
}
CATEGORIES = list(SEGMENT_MAP)
CHANNELS = ["in_store", "online", "mobile", "contactless"]
STATES = ["NY", "CA", "TX", "FL", "IL", "WA", "GA", "MA", "NJ", "PA"]
CITIES = {
    "NY": "New York", "CA": "Los Angeles", "TX": "Houston", "FL": "Miami",
    "IL": "Chicago", "WA": "Seattle", "GA": "Atlanta", "MA": "Boston",
    "NJ": "Jersey City", "PA": "Philadelphia",
}
# Plausible merchant names per category (dim_merchant.merchant_name)
MERCHANT_NAMES = {
    "grocery": ["FreshMart", "GreenBasket", "DailyGrocer"],
    "restaurant": ["Saffron Table", "Burger Barn", "Noodle House"],
    "gas_station": ["FuelStop", "Highway Gas Co"],
    "pharmacy": ["CarePlus Pharmacy", "MediMart"],
    "healthcare": ["City Clinic", "WellPath Health"],
    "retail_clothing": ["Urban Thread", "StyleHub"],
    "electronics": ["TechNova", "CircuitWorld"],
    "entertainment": ["CinePlex", "GameZone Arcade"],
    "travel_hotel": ["Grand Stay Hotels", "Skyline Inn"],
    "travel_airline": ["AeroJet", "SkyBridge Air"],
    "subscription_services": ["StreamPlus", "CloudBox Pro"],
    "atm": ["QuickCash ATM", "Metro ATM Network"],
}
AVG_TICKET = {
    "grocery": 62, "restaurant": 38, "gas_station": 45, "pharmacy": 54,
    "healthcare": 210, "retail_clothing": 88, "electronics": 340,
    "entertainment": 46, "travel_hotel": 320, "travel_airline": 410,
    "subscription_services": 18, "atm": 120,
}

END_DATE = pd.Timestamp("2026-11-06")
N_DAYS = 90


def _merchants(rng: np.random.Generator) -> pd.DataFrame:
    rows = []
    mid = 0
    for cat, names in MERCHANT_NAMES.items():
        for name in names:
            mid += 1
            state = str(rng.choice(STATES))
            rows.append({
                "merchant_id": f"M{mid:03d}",
                "merchant_name": name,
                "merchant_category": cat,
                "merchant_segment": SEGMENT_MAP[cat],
                "city": CITIES[state],
                "state": state,
                "popularity": float(rng.uniform(0.5, 1.8)),
            })
    return pd.DataFrame(rows)


def build_all() -> dict[str, pd.DataFrame]:
    """Build every console dataset deterministically (seed 42)."""
    rng = np.random.default_rng(42)
    merchants = _merchants(rng)
    dates = pd.date_range(end=END_DATE, periods=N_DAYS, freq="D")

    # ---- Individual transactions (fact_transactions grain) ----------------
    n_txn = 9000
    day_idx = rng.integers(0, N_DAYS, n_txn)
    # Weekend + slow upward trend, like real commerce volume
    dow = np.array([dates[i].dayofweek for i in day_idx])
    merch_pick = rng.choice(
        len(merchants), n_txn,
        p=(merchants["popularity"] / merchants["popularity"].sum()).to_numpy(),
    )
    m = merchants.iloc[merch_pick].reset_index(drop=True)
    base_ticket = m["merchant_category"].map(AVG_TICKET).to_numpy(dtype=float)
    amount = np.round(rng.lognormal(np.log(base_ticket), 0.55), 2)
    amount = np.clip(amount, 1.0, 4999.0)

    customers = np.array([f"CUST_{i:04d}" for i in range(1, 401)])
    cust_pick = rng.choice(len(customers), n_txn)

    # Velocity features (enrich.py semantics): most traffic is calm, a
    # minority arrives in bursts that push the 1h / 24h windows up.
    burst = rng.random(n_txn) < 0.16
    txn_1h = np.where(
        burst, rng.integers(8, 26, n_txn), rng.integers(1, 6, n_txn)
    ).astype(int)
    distinct_1h = np.minimum(
        txn_1h, np.where(burst, rng.integers(2, 9, n_txn), rng.integers(1, 4, n_txn))
    ).astype(int)
    spend_24h = np.round(amount * rng.uniform(1.0, 3.5, n_txn)
                         + txn_1h * rng.uniform(20, 160, n_txn), 2)
    spend_24h = np.where(burst, spend_24h * rng.uniform(1.6, 3.2, n_txn), spend_24h)

    event_time = pd.to_datetime(dates[day_idx]) + pd.to_timedelta(
        rng.integers(0, 24 * 3600, n_txn), unit="s"
    )
    txns = pd.DataFrame({
        "transaction_id": [f"txn-{i:06d}" for i in range(n_txn)],
        "event_time": event_time,
        "txn_date": pd.to_datetime(dates[day_idx]),
        "customer_id": customers[cust_pick],
        "merchant_id": m["merchant_id"].to_numpy(),
        "merchant_name": m["merchant_name"].to_numpy(),
        "merchant_category": m["merchant_category"].to_numpy(),
        "merchant_segment": m["merchant_segment"].to_numpy(),
        "state": m["state"].to_numpy(),
        "city": m["city"].to_numpy(),
        "amount_usd": amount,
        "channel": rng.choice(CHANNELS, n_txn, p=[0.34, 0.28, 0.22, 0.16]),
        "txn_count_last_1h": txn_1h,
        "distinct_merchants_1h": distinct_1h,
        "spend_velocity_24h": np.round(spend_24h, 2),
        "is_international": rng.random(n_txn) < 0.09,
        "hour_of_day": pd.Series(event_time).dt.hour.to_numpy(),
    })
    # enrich.py derivations, verbatim thresholds
    txns["cross_merchant_flag"] = txns["distinct_merchants_1h"] >= 5
    txns["high_velocity_flag"] = txns["txn_count_last_1h"] > 10
    txns["risk_tier"] = np.select(
        [txns["spend_velocity_24h"] >= 5000, txns["spend_velocity_24h"] >= 1000],
        ["high", "medium"], default="low",
    )
    n_flags = (txns["cross_merchant_flag"].astype(int)
               + txns["high_velocity_flag"].astype(int)
               + txns["is_international"].astype(int)
               + (txns["risk_tier"] == "high").astype(int))
    txns["alert_score"] = (1 + n_flags).clip(upper=4).astype(int)
    txns["signal_label"] = np.select(
        [txns["cross_merchant_flag"], txns["high_velocity_flag"], txns["is_international"]],
        ["high_cross_merchant", "high_velocity", "medium_intl"], default="low",
    )

    # ---- Merchant daily performance (merchant_daily_performance_v) --------
    daily = (
        txns.groupby(
            ["txn_date", "merchant_name", "merchant_category",
             "merchant_segment", "state"], as_index=False
        ).agg(txn_count=("transaction_id", "count"),
              total_revenue_usd=("amount_usd", "sum"),
              avg_ticket_usd=("amount_usd", "mean"))
    )
    daily["total_revenue_usd"] = daily["total_revenue_usd"].round(2)
    daily["avg_ticket_usd"] = daily["avg_ticket_usd"].round(2)
    daily = daily.sort_values(["merchant_name", "txn_date"])
    daily["revenue_rolling_7d"] = (
        daily.groupby("merchant_name")["total_revenue_usd"]
        .transform(lambda s: s.rolling(7, min_periods=1).sum()).round(2)
    )
    daily["revenue_dod_delta"] = (
        daily.groupby("merchant_name")["total_revenue_usd"].diff().fillna(0).round(2)
    )

    # ---- Fraud alerts (fraud_velocity_alerts_v: flagged rows only) --------
    fraud = txns[
        txns["high_velocity_flag"] | txns["cross_merchant_flag"]
        | txns["is_international"] | (txns["risk_tier"] == "high")
    ].copy()

    # ---- Customer spend profiles (customer_spend_profile_v) ---------------
    cust = (
        txns.groupby("customer_id").agg(
            lifetime_txn_count=("transaction_id", "count"),
            lifetime_spend_usd=("amount_usd", "sum"),
            avg_txn_usd=("amount_usd", "mean"),
            last_txn_time=("event_time", "max"),
            first_txn_time=("event_time", "min"),
            distinct_merchants_visited=("merchant_id", "nunique"),
            cross_merchant_events=("cross_merchant_flag", "sum"),
            intl_txn_count=("is_international", "sum"),
        ).reset_index()
    )
    # Lifetime = pre-window history + the 90-day window above, mirroring
    # customer_spend_profile_v aggregating the full fact table (the view's
    # $50k/$10k/$1k segment thresholds are lifetime, not 90-day, figures).
    rng_c = np.random.default_rng(7)
    baseline_spend = np.round(rng_c.lognormal(np.log(5000), 1.3, len(cust)), 2)
    baseline_txns = np.maximum(
        1, (baseline_spend / cust["avg_txn_usd"].to_numpy()).astype(int)
    )
    cust["lifetime_spend_usd"] = (cust["lifetime_spend_usd"] + baseline_spend).round(2)
    cust["lifetime_txn_count"] = cust["lifetime_txn_count"] + baseline_txns
    cust["avg_txn_usd"] = (
        cust["lifetime_spend_usd"] / cust["lifetime_txn_count"]
    ).round(2)
    active_days = (cust["last_txn_time"] - cust["first_txn_time"]).dt.days.clip(lower=1)
    cust["recency_days"] = (END_DATE - cust["last_txn_time"].dt.normalize()).dt.days
    window_txns = cust["lifetime_txn_count"] - baseline_txns
    cust["txn_frequency_per_day"] = (window_txns / active_days).round(2)
    cust["account_type"] = rng.choice(
        ["checking", "credit", "debit"], len(cust), p=[0.45, 0.35, 0.20]
    )
    worst = txns.groupby("customer_id")["spend_velocity_24h"].max()
    cust["risk_tier"] = cust["customer_id"].map(
        lambda c: "high" if worst.get(c, 0) >= 5000
        else ("medium" if worst.get(c, 0) >= 1000 else "low")
    )
    cust["spend_segment"] = np.select(
        [cust["lifetime_spend_usd"] >= 50000, cust["lifetime_spend_usd"] >= 10000,
         cust["lifetime_spend_usd"] >= 1000],
        ["platinum", "gold", "silver"], default="bronze",
    )

    # ---- Pipeline health (daily batch series) ------------------------------
    pipe = pd.DataFrame({"txn_date": dates})
    pipe["records_loaded"] = rng.integers(38_000, 118_000, N_DAYS)
    pipe["quarantine_rate_pct"] = np.round(rng.uniform(0.1, 2.4, N_DAYS), 2)
    pipe["load_latency_s"] = np.round(rng.uniform(4.0, 18.0, N_DAYS), 1)
    pipe["dlq_count"] = rng.integers(0, 12, N_DAYS)
    pipe["schema_violations"] = rng.integers(0, 5, N_DAYS)

    return {
        "merchants": merchants, "transactions": txns, "merchant_daily": daily,
        "fraud_alerts": fraud, "customers": cust, "pipeline_daily": pipe,
    }
