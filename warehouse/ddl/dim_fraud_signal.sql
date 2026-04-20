-- dim_fraud_signal.sql
-- Conformed dimension describing the fraud signal profile of a transaction.
-- One row per distinct combination of signal flags — small cardinality table.

CREATE TABLE IF NOT EXISTS connected_commerce.dim_fraud_signal (
    fraud_signal_key        INT         NOT NULL AUTOINCREMENT PRIMARY KEY,
    cross_merchant_flag     BOOLEAN     NOT NULL,
    is_international        BOOLEAN     NOT NULL,
    risk_tier               VARCHAR(10) NOT NULL,
    high_velocity_flag      BOOLEAN     NOT NULL,   -- txn_count_last_1h > 10
    signal_label            VARCHAR(50) NOT NULL,   -- human-readable composite label
    CONSTRAINT uq_fraud_signal
        UNIQUE (cross_merchant_flag, is_international, risk_tier, high_velocity_flag)
);

-- Pre-populate all combinations
INSERT INTO connected_commerce.dim_fraud_signal
    (cross_merchant_flag, is_international, risk_tier, high_velocity_flag, signal_label)
SELECT
    cross_merchant_flag,
    is_international,
    risk_tier,
    high_velocity_flag,
    risk_tier
        || CASE WHEN cross_merchant_flag  THEN '_cross_merchant' ELSE '' END
        || CASE WHEN is_international     THEN '_intl'           ELSE '' END
        || CASE WHEN high_velocity_flag   THEN '_high_velocity'  ELSE '' END
        AS signal_label
FROM (
    SELECT col1 AS cross_merchant_flag,
           col2 AS is_international,
           col3 AS risk_tier,
           col4 AS high_velocity_flag
    FROM VALUES
        (TRUE,  TRUE,  'high',   TRUE),
        (TRUE,  TRUE,  'high',   FALSE),
        (TRUE,  TRUE,  'medium', TRUE),
        (TRUE,  TRUE,  'medium', FALSE),
        (TRUE,  TRUE,  'low',    TRUE),
        (TRUE,  TRUE,  'low',    FALSE),
        (TRUE,  FALSE, 'high',   TRUE),
        (TRUE,  FALSE, 'high',   FALSE),
        (TRUE,  FALSE, 'medium', TRUE),
        (TRUE,  FALSE, 'medium', FALSE),
        (TRUE,  FALSE, 'low',    TRUE),
        (TRUE,  FALSE, 'low',    FALSE),
        (FALSE, TRUE,  'high',   TRUE),
        (FALSE, TRUE,  'high',   FALSE),
        (FALSE, TRUE,  'medium', TRUE),
        (FALSE, TRUE,  'medium', FALSE),
        (FALSE, TRUE,  'low',    TRUE),
        (FALSE, TRUE,  'low',    FALSE),
        (FALSE, FALSE, 'high',   TRUE),
        (FALSE, FALSE, 'high',   FALSE),
        (FALSE, FALSE, 'medium', TRUE),
        (FALSE, FALSE, 'medium', FALSE),
        (FALSE, FALSE, 'low',    TRUE),
        (FALSE, FALSE, 'low',    FALSE)
);