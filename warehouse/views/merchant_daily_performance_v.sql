-- merchant_daily_performance_v.sql
-- Daily merchant revenue, transaction volume, and average ticket size.
-- Primary view for the Streamlit merchant KPI dashboard.

CREATE OR REPLACE VIEW connected_commerce.merchant_daily_performance_v AS
SELECT
    dd.full_date                            AS txn_date,
    dd.month_name,
    dd.quarter,
    dd.year,
    dm.merchant_id,
    dm.merchant_name,
    dm.merchant_category,
    dm.merchant_segment,
    dm.state,

    COUNT(ft.transaction_sk)                AS txn_count,
    SUM(ft.amount_usd)                      AS total_revenue_usd,
    AVG(ft.amount_usd)                      AS avg_ticket_usd,
    MAX(ft.amount_usd)                      AS max_ticket_usd,
    MIN(ft.amount_usd)                      AS min_ticket_usd,

    -- 7-day rolling revenue per merchant
    SUM(SUM(ft.amount_usd)) OVER (
        PARTITION BY dm.merchant_id
        ORDER BY dd.full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                       AS revenue_rolling_7d,

    -- Day-over-day revenue change
    LAG(SUM(ft.amount_usd)) OVER (
        PARTITION BY dm.merchant_id
        ORDER BY dd.full_date
    )                                       AS prev_day_revenue,

    SUM(ft.amount_usd)
        - LAG(SUM(ft.amount_usd)) OVER (
            PARTITION BY dm.merchant_id
            ORDER BY dd.full_date
          )                                 AS revenue_dod_delta

FROM connected_commerce.fact_transactions ft
JOIN connected_commerce.dim_merchant dm ON ft.merchant_key  = dm.merchant_key
JOIN connected_commerce.dim_date     dd ON ft.date_key      = dd.date_key
GROUP BY
    dd.full_date, dd.month_name, dd.quarter, dd.year,
    dm.merchant_id, dm.merchant_name, dm.merchant_category,
    dm.merchant_segment, dm.state;