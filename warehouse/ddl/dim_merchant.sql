-- dim_merchant.sql
-- SCD Type 1 merchant dimension.

CREATE TABLE IF NOT EXISTS connected_commerce.dim_merchant (
    merchant_key      INT           NOT NULL AUTOINCREMENT PRIMARY KEY,
    merchant_id       VARCHAR(50)   NOT NULL UNIQUE,
    merchant_name     VARCHAR(255),
    merchant_category VARCHAR(50),
    merchant_segment  VARCHAR(50),
    city              VARCHAR(100),
    state             VARCHAR(5),
    dw_created_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dw_updated_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE connected_commerce.upsert_dim_merchant()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO connected_commerce.dim_merchant tgt
USING (
    SELECT DISTINCT
        merchant_id,
        merchant_name,
        merchant_category,
        merchant_segment,
        city,
        state
    FROM connected_commerce.transactions_stage
) src
ON tgt.merchant_id = src.merchant_id
WHEN MATCHED THEN UPDATE SET
    merchant_name     = src.merchant_name,
    merchant_category = src.merchant_category,
    merchant_segment  = src.merchant_segment,
    city              = src.city,
    state             = src.state,
    dw_updated_at     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    merchant_id, merchant_name, merchant_category, merchant_segment, city, state
) VALUES (
    src.merchant_id, src.merchant_name, src.merchant_category,
    src.merchant_segment, src.city, src.state
);
$$;