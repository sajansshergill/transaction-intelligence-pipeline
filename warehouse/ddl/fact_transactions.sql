-- fact_transactions.sql
-- Central fact table. Append-only — no updates after insert.
-- Clustered on (ingest_date, merchant_key) for common query patterns.

CREATE TABLE IF NOT EXISTS connected_commerce.fact_transactions (
    -- Surrogate keys
    transaction_sk    INT           NOT NULL AUTOINCREMENT PRIMARY KEY,
    customer_key      INT           NOT NULL REFERENCES connected_commerce.dim_customer(customer_key),
    merchant_key      INT           NOT NULL REFERENCES connected_commerce.dim_merchant(merchant_key),
    date_key          INT           NOT NULL REFERENCES connected_commerce.dim_date(date_key),
    fraud_signal_key  INT           NOT NULL REFERENCES connected_commerce.dim_fraud_signal(fraud_signal_key),

    -- Natural key (idempotency guard)
    transaction_id    VARCHAR(36)   NOT NULL UNIQUE,

    -- Measures
    amount_usd              DECIMAL(12, 2) NOT NULL,
    txn_count_last_1h       INT,
    distinct_merchants_1h   INT,
    spend_velocity_24h      DECIMAL(12, 2),
    hour_of_day             INT,
    day_of_week             INT,

    -- Degenerate dimensions
    transaction_type  VARCHAR(20),
    channel           VARCHAR(20),
    currency          VARCHAR(5)    DEFAULT 'USD',

    -- Audit
    event_time        TIMESTAMP_NTZ,
    ingest_date       DATE,
    dw_loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ingest_date, merchant_key);
