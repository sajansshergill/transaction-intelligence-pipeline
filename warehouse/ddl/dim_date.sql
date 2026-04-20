-- dim_date.sql
-- Date dimension pre-populated for 2020–2030.
-- Generated once via a stored procedure or dbt seed; never updated by the pipeline.

CREATE TABLE IF NOT EXISTS connected_commerce.dim_date (
    date_key        INT           NOT NULL,   -- YYYYMMDD integer surrogate key
    full_date       DATE          NOT NULL,
    day_of_week     INT           NOT NULL,   -- 1=Sunday … 7=Saturday
    day_name        VARCHAR(10)   NOT NULL,
    day_of_month    INT           NOT NULL,
    day_of_year     INT           NOT NULL,
    week_of_year    INT           NOT NULL,
    month_num       INT           NOT NULL,
    month_name      VARCHAR(10)   NOT NULL,
    quarter         INT           NOT NULL,
    year            INT           NOT NULL,
    is_weekend      BOOLEAN       NOT NULL,
    is_holiday      BOOLEAN       NOT NULL DEFAULT FALSE,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

-- Populate 2020–2030
INSERT INTO connected_commerce.dim_date
WITH date_spine AS (
    SELECT DATEADD('day', SEQ4(), '2020-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 3653))   -- 10 years
)
SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD'))  AS date_key,
    full_date,
    DAYOFWEEK(full_date)                       AS day_of_week,
    DAYNAME(full_date)                         AS day_name,
    DAY(full_date)                             AS day_of_month,
    DAYOFYEAR(full_date)                       AS day_of_year,
    WEEKOFYEAR(full_date)                      AS week_of_year,
    MONTH(full_date)                           AS month_num,
    MONTHNAME(full_date)                       AS month_name,
    QUARTER(full_date)                         AS quarter,
    YEAR(full_date)                            AS year,
    DAYOFWEEK(full_date) IN (1, 7)             AS is_weekend,
    FALSE                                      AS is_holiday
FROM date_spine;