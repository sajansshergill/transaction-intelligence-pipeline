-- customer_spend_profile_v.sql
-- Per-customer lifetime spend profile with recency, frequency, monetary (RFM) signals.

CREATE OR REPLACE VIEW connected_commerce.customer_spend_profile_v AS
WITH customer_agg AS (
    SELECT
        dc.customer_id,
        dc.account_type,
        dc.risk_tier,
        COUNT(ft.transaction_sk)            AS lifetime_txn_count,
        SUM(ft.amount_usd)                  AS lifetime_spend_usd,
        AVG(ft.amount_usd)                  AS avg_txn_usd,
        MAX(ft.event_time)                  AS last_txn_time,
        MIN(ft.event_time)                  AS first_txn_time,
        DATEDIFF('day',
            MIN(ft.event_time)::DATE,
            MAX(ft.event_time)::DATE
        )                                   AS active_days,
        COUNT(DISTINCT dm.merchant_id)      AS distinct_merchants_visited,
        COUNT(DISTINCT dm.merchant_category) AS distinct_categories_visited,
        SUM(CASE WHEN dfs.cross_merchant_flag  THEN 1 ELSE 0 END) AS cross_merchant_events,
        SUM(CASE WHEN dfs.is_international     THEN 1 ELSE 0 END) AS intl_txn_count
    FROM connected_commerce.fact_transactions ft
    JOIN connected_commerce.dim_customer     dc  ON ft.customer_key     = dc.customer_key
    JOIN connected_commerce.dim_merchant     dm  ON ft.merchant_key     = dm.merchant_key
    JOIN connected_commerce.dim_fraud_signal dfs ON ft.fraud_signal_key = dfs.fraud_signal_key
    GROUP BY dc.customer_id, dc.account_type, dc.risk_tier
)
SELECT
    *,
    -- RFM proxies
    DATEDIFF('day', last_txn_time::DATE, CURRENT_DATE())    AS recency_days,
    ROUND(lifetime_txn_count / NULLIF(active_days, 0), 2)   AS txn_frequency_per_day,

    -- Spend segment
    CASE
        WHEN lifetime_spend_usd >= 50000 THEN 'platinum'
        WHEN lifetime_spend_usd >= 10000 THEN 'gold'
        WHEN lifetime_spend_usd >=  1000 THEN 'silver'
        ELSE 'bronze'
    END AS spend_segment

FROM customer_agg;