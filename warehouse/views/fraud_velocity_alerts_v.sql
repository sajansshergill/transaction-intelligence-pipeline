-- fraud_velocity_alerts_v.sql
-- Near-real-time fraud alert surface focused on velocity + risky signal patterns.
-- Intended for dashboards/ops triage: one row per flagged transaction.

CREATE OR REPLACE VIEW connected_commerce.fraud_velocity_alerts_v AS
SELECT
    ft.event_time,
    ft.ingest_date,
    dc.customer_id,
    dc.account_type,
    dc.risk_tier                           AS customer_risk_tier,
    dm.merchant_id,
    dm.merchant_name,
    dm.merchant_category,
    dm.merchant_segment,
    dm.city,
    dm.state,

    ft.transaction_id,
    ft.amount_usd,
    ft.currency,
    ft.channel,
    ft.transaction_type,

    ft.txn_count_last_1h,
    ft.distinct_merchants_1h,
    ft.spend_velocity_24h,

    dfs.cross_merchant_flag,
    dfs.is_international,
    dfs.high_velocity_flag,
    dfs.signal_label,

    CASE
        WHEN dfs.risk_tier = 'high'
         AND (dfs.high_velocity_flag OR dfs.cross_merchant_flag OR dfs.is_international)
            THEN 'critical'
        WHEN dfs.high_velocity_flag OR dfs.cross_merchant_flag
            THEN 'high'
        WHEN dfs.is_international OR dfs.risk_tier = 'high'
            THEN 'medium'
        ELSE 'low'
    END AS alert_severity
FROM connected_commerce.fact_transactions ft
JOIN connected_commerce.dim_customer     dc  ON ft.customer_key     = dc.customer_key
JOIN connected_commerce.dim_merchant     dm  ON ft.merchant_key     = dm.merchant_key
JOIN connected_commerce.dim_fraud_signal dfs ON ft.fraud_signal_key = dfs.fraud_signal_key
WHERE
    dfs.high_velocity_flag
 OR dfs.cross_merchant_flag
 OR dfs.is_international
 OR dfs.risk_tier = 'high';