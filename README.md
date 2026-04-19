# Connected Commerce Transaction Intelligence Pipeline

Real-time fraud signal aggregation and merchant analytics -> Kafka -> S3 -> PySpark -> Snowflake -> Streamlit

## Overview
This project simulates a production-grade payment data platfrom modeled after the architecture used by large-scale Connected Commerce teams. It demonstrates the full data engineering lifecycle —— from synthetic transaction event generation through Kafka, to curated Parquet/Iceberg tables on S3, to analytical star schema in Snowflake, surfaced through a real-time Streamlit dashboard.

**Core focus areas:** Kafka + S3 intergration, PySpark batch processing, Kimball dimensional modeling in Snowflake, data quality enforcement, CI/CD with Github Actions.

## Architecture
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                              │
│                                                                 │
│   Kafka Producer (Faker)  ──►  Kafka Topic: raw_transactions    │
│         [AVRO Schema + Schema Registry]                         │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LANDING LAYER (S3)                           │
│                                                                 │
│   Kafka Consumer  ──►  s3://bucket/raw/transactions/            │
│                         Partitioned by date/hour                │
│                         Format: AVRO                            │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PROCESSING LAYER (PySpark)                   │
│                                                                 │
│   ┌─────────────────────────────────────────────────────┐       │
│   │  Schema Validation  →  Null/Type Checks             │       │
│   │  Great Expectations data quality suite              │       │
│   │  Fraud Velocity Features:                           │       │
│   │    - txn_count_last_1h per customer                 │       │
│   │    - spend_velocity_last_24h per merchant           │       │
│   │    - cross_merchant_flag (card used at 5+ merchants)│       │
│   │  Merchant Category Enrichment (lookup join)         │       │
│   └──────────────────────┬──────────────────────────────┘       │
│                          │                                      │
│                          ▼                                      │
│   s3://bucket/curated/transactions/                             │
│   Format: Parquet (Iceberg table format)                        │
│   Partitioned by: year / month / day                            │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SERVING LAYER (Snowflake)                    │
│                                                                 │
│   COPY INTO  ──►  Kimball Star Schema                           │
│                                                                 │
│   fact_transactions                                             │
│     ├── dim_customer                                            │
│     ├── dim_merchant                                            │
│     ├── dim_date                                                │
│     └── dim_fraud_signal                                        │
│                                                                 │
│   Analytical Views:                                             │
│     - merchant_daily_performance_v                              │
│     - fraud_velocity_alerts_v                                   │
│     - customer_spend_profile_v                                  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    VISUALIZATION (Streamlit)                    │
│                                                                 │
│   • Merchant Performance Dashboard                              │
│   • Fraud Velocity Monitor (real-time alerts)                   │
│   • Pipeline Health & Data Quality Scorecard                    │
└─────────────────────────────────────────────────────────────────┘

## Repository Structure
<img width="376" height="605" alt="image" src="https://github.com/user-attachments/assets/570b6b02-37d0-4a58-a36b-cca38e890b92" />

<img width="376" height="402" alt="image" src="https://github.com/user-attachments/assets/2dd023a9-128a-480b-9901-9a4e1b914d24" />

## Tech Stack
<img width="347" height="300" alt="image" src="https://github.com/user-attachments/assets/6d895779-4e38-405d-9265-10d110a2e6c0" />

**Data Model —— Kimball Star Schema**
fact_transactions
<img width="372" height="269" alt="image" src="https://github.com/user-attachments/assets/71664123-88e7-44f9-af36-fbf1a839e5e5" />

dim_merchant
<img width="332" height="173" alt="image" src="https://github.com/user-attachments/assets/deaa758a-9386-42c1-9d28-3d607c276618" />

dim_customer
<img width="320" height="156" alt="image" src="https://github.com/user-attachments/assets/883db6f1-2bcb-4768-96c2-800f1d9da50e" />

## Key Engineering Decisions
**Why AVRO for raw, Parquet/Iceberg for curated?** AVRO is schema-enforced and compact for high-throughput Kafka streaming, Parquet with Iceberg gives columnar compression, time-travel, and schema evolution for batch analytics —— aligning with how production lakehouse pipelines are structured.

**Why Kimball star schema in Snowflake?** JPMC's analytics teams query for business-level aggregations (merchnat performance, customer spend trends). A denormalized star schema minimizes join complexity and plays to Snowflake's columnar execution engine. 

**Velocity features in PySpark —— not SQL** Computing txn_count_last_1h and spend_velocity_24h as window functions at the PySpark layer (before load) keeps Snowflake serving analytical reads only. The PySpark job uses Window.partitionBy("customer_id").orderBy("event_time").rangeBetween(-3600, 0).

**Micro-batch vs. batch** The Kafka consumer runs as a micro-batch (5-minyte flush to S3), while the PySpark enrichment job runs as a scheduled hourly batch via run_pipeline.sh. 

## Setup & Run
**Prerequisites**
- Docker + Docker Compose
- Python 3.11+
- AWS credentials configured (~/.aws/credentials)
- Snowflake account + warehouse

### 1. Start Kafka locally
bashdocker-compose up -d
bash scripts/setup_kafka.sh

### 2. Install dependencies
bashpip install -r requirements.txt

### 3. Bootstrap Snowflake objects
bashbash scripts/setup_snowflake.sh

### 4. Run end-to-end pipeline
bashbash scripts/run_pipeline.sh
This runs: producer → consumer → PySpark validate → enrich → write Iceberg → Snowflake COPY INTO

### 5. Launch dashboard
bashstreamlit run dashboard/app.py

### Testing
bashpytest tests/ -v --tb=short

Tests cover: AVRO schema enforcement, PySpark schema validation logic, velocity feature computation correctness, S3 path partitioning, Snowflake DDL idempotency.

## CI/CD
GitHub Actions runs on every push to main and every pull request:
- flake8 linting
- pytest full test suite
- Schema compatibility check against AVRO registry mock

See .github/workflows/ci.yml.

**Data Quality Checks (Great Expectations)**
<img width="510" height="235" alt="image" src="https://github.com/user-attachments/assets/001c0a98-d5d8-4b62-bb9a-cd07b62cd1eb" />

## Sample Queries (Snowflake)
**Top 10 merchants by revenue —— last 30 days**

SELECT
  dm.merchant_name,
  dm.merchant_category,
  SUM(ft.amount_usd) AS total_revenue,
  COUNT(ft.transaction_id) AS txn_count
FROM fact_transactions ft
JOIN dim_merchant dm ON ft.merchant_key = 
