-- copy_into.sql
-- Full batch load sequence run after each PySpark Iceberg write.
-- Order: stage → dims (upsert) → fact (insert, dedup guard)

-- ── 1. External stage pointing to curated S3 prefix ──────────────────────────
CREATE STAGE IF NOT EXISTS connected_commerce.s3_curated_stage
    URL = 's3://connected-commerce-lake/curated/transactions/'
    CREDENTIALS = (AWS_ROLE = '<your-iam-role-arn>')
    FILE_FORMAT = (
        TYPE            = 'PARQUET'
        COMPRESSION     = 'SNAPPY'
    );

-- ── 2. Staging table (raw landing inside Snowflake) ───────────────────────────
CREATE TABLE IF NOT EXISTS connected_commerce.transactions_stage (
    transaction_id        VARCHAR(36),
    customer_id           VARCHAR(50),
    merchant_id           VARCHAR(50),
    merchant_name         VARCHAR(255),
    merchant_category     VARCHAR(50),
    merchant_segment      VARCHAR(50),
    city                  VARCHAR(100),
    state                 VARCHAR(5),
    amount_usd            DECIMAL(12,2),
    currency              VARCHAR(5),
    account_type          VARCHAR(20),
    transaction_type      VARCHAR(20),
    channel               VARCHAR(20),
    is_international      BOOLEAN,
    zip_code              VARCHAR(10),
    event_time            TIMESTAMP_NTZ,
    ingest_date           DATE,
    hour_of_day           INT,
    day_of_week           INT,
    week_of_year          INT,
    txn_count_last_1h     INT,
    distinct_merchants_1h INT,
    spend_velocity_24h    DECIMAL(12,2),
    cross_merchant_flag   BOOLEAN,
    risk_tier             VARCHAR(10)
);

-- ── 3. COPY INTO stage from S3 ────────────────────────────────────────────────
COPY INTO connected_commerce.transactions_stage
FROM @connected_commerce.s3_curated_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PURGE = FALSE
ON_ERROR = 'CONTINUE';   -- log bad files, don't abort the batch

-- ── 4. Upsert dimensions ──────────────────────────────────────────────────────
CALL connected_commerce.upsert_dim_customer();
CALL connected_commerce.upsert_dim_merchant();

-- ── 5. Insert into fact (skip duplicates via transaction_id uniqueness) ────────
INSERT INTO connected_commerce.fact_transactions (
    customer_key,
    merchant_key,
    date_key,
    fraud_signal_key,
    transaction_id,
    amount_usd,
    txn_count_last_1h,
    distinct_merchants_1h,
    spend_velocity_24h,
    hour_of_day,
    day_of_week,
    transaction_type,
    channel,
    currency,
    event_time,
    ingest_date
)
SELECT
    dc.customer_key,
    dm.merchant_key,
    dd.date_key,
    dfs.fraud_signal_key,
    s.transaction_id,
    s.amount_usd,
    s.txn_count_last_1h,
    s.distinct_merchants_1h,
    s.spend_velocity_24h,
    s.hour_of_day,
    s.day_of_week,
    s.transaction_type,
    s.channel,
    s.currency,
    s.event_time,
    s.ingest_date
FROM connected_commerce.transactions_stage s

JOIN connected_commerce.dim_customer dc
    ON s.customer_id = dc.customer_id

JOIN connected_commerce.dim_merchant dm
    ON s.merchant_id = dm.merchant_id

JOIN connected_commerce.dim_date dd
    ON TO_NUMBER(TO_CHAR(s.ingest_date, 'YYYYMMDD')) = dd.date_key

JOIN connected_commerce.dim_fraud_signal dfs
    ON s.cross_merchant_flag                        = dfs.cross_merchant_flag
   AND s.is_international                           = dfs.is_international
   AND s.risk_tier                                  = dfs.risk_tier
   AND (s.txn_count_last_1h > 10)                  = dfs.high_velocity_flag

-- Idempotency: skip rows already loaded
WHERE s.transaction_id NOT IN (
    SELECT transaction_id FROM connected_commerce.fact_transactions
);

-- ── 6. Truncate stage after successful load ───────────────────────────────────
TRUNCATE TABLE connected_commerce.transactions_stage;