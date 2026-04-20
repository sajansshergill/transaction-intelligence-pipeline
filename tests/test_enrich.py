"""
tests/test_enrich.py — Unit tests for velocity features and enrichment.
"""

import pytest
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.errors.exceptions.base import PySparkRuntimeError

from processing.jobs.enrich import (
    add_time_features,
    add_velocity_features,
    add_risk_tier,
    add_merchant_segment,
    RISK_TIER_HIGH,
    RISK_TIER_MEDIUM,
)


@pytest.fixture(scope="module")
def spark():
    try:
        return (
            SparkSession.builder
            .master("local[1]")
            .appName("test_enrich")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except PySparkRuntimeError as e:
        # Common local-dev failure: Java runtime not installed/configured.
        pytest.skip(f"Skipping Spark tests (Spark JVM failed to start): {e}")


def _ts(minutes_ago: int = 0) -> str:
    dt = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _make_df(spark, rows):
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, DateType
    )
    schema = StructType([
        StructField("transaction_id",    StringType(),   True),
        StructField("customer_id",       StringType(),   True),
        StructField("merchant_id",       StringType(),   True),
        StructField("merchant_category", StringType(),   True),
        StructField("amount_usd",        DoubleType(),   True),
        StructField("event_time",        StringType(),   True),
        StructField("event_time_ts",     TimestampType(),True),
        StructField("ingest_date",       DateType(),     True),
    ])
    return spark.createDataFrame(rows, schema)


class TestTimeFeatures:

    def test_hour_of_day_extracted(self, spark):
        from pyspark.sql import Row
        rows = [(
            "txn-1", "C1", "M1", "grocery", 10.0,
            "2025-04-12T14:30:00+00:00",
            datetime(2025, 4, 12, 14, 30, 0, tzinfo=timezone.utc),
            datetime(2025, 4, 12).date(),
        )]
        df     = _make_df(spark, rows)
        result = add_time_features(df)
        row    = result.collect()[0]
        assert row["hour_of_day"]  == 14
        assert row["day_of_week"]  == 7   # Saturday = 7

    def test_week_of_year_present(self, spark):
        rows = [(
            "txn-1", "C1", "M1", "grocery", 10.0,
            "2025-01-01T00:00:00+00:00",
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2025, 1, 1).date(),
        )]
        df     = _make_df(spark, rows)
        result = add_time_features(df)
        assert "week_of_year" in result.columns


class TestVelocityFeatures:

    def test_txn_count_last_1h_counts_correctly(self, spark):
        now = datetime.now(timezone.utc)
        rows = [
            ("t1", "C1", "M1", "grocery", 10.0, "", now - timedelta(minutes=50), None),
            ("t2", "C1", "M2", "grocery", 20.0, "", now - timedelta(minutes=30), None),
            ("t3", "C1", "M3", "grocery", 30.0, "", now - timedelta(minutes=10), None),
            ("t4", "C2", "M1", "grocery", 50.0, "", now - timedelta(minutes=5),  None),
        ]
        schema_cols = [
            "transaction_id","customer_id","merchant_id","merchant_category",
            "amount_usd","event_time","event_time_ts","ingest_date"
        ]
        df = spark.createDataFrame(rows, schema_cols)
        result = add_velocity_features(df)
        c1_rows = result.filter(F.col("customer_id") == "C1").orderBy("event_time_ts").collect()
        # All 3 C1 transactions are within 1 hour of each other
        assert c1_rows[-1]["txn_count_last_1h"] == 3
        c2_rows = result.filter(F.col("customer_id") == "C2").collect()
        assert c2_rows[0]["txn_count_last_1h"] == 1

    def test_cross_merchant_flag_triggers_at_5(self, spark):
        now  = datetime.now(timezone.utc)
        rows = [
            (f"t{i}", "C1", f"M{i}", "grocery", 5.0, "", now - timedelta(minutes=i*5), None)
            for i in range(1, 7)   # 6 distinct merchants
        ]
        schema_cols = [
            "transaction_id","customer_id","merchant_id","merchant_category",
            "amount_usd","event_time","event_time_ts","ingest_date"
        ]
        df     = spark.createDataFrame(rows, schema_cols)
        result = add_velocity_features(df)
        last   = result.orderBy(F.desc("event_time_ts")).collect()[0]
        assert last["cross_merchant_flag"] is True


class TestRiskTier:

    def test_high_risk_above_threshold(self, spark):
        from pyspark.sql import Row
        df = spark.createDataFrame([Row(spend_velocity_24h=6000.0)])
        result = add_risk_tier(df)
        assert result.collect()[0]["risk_tier"] == "high"

    def test_medium_risk(self, spark):
        from pyspark.sql import Row
        df = spark.createDataFrame([Row(spend_velocity_24h=1500.0)])
        result = add_risk_tier(df)
        assert result.collect()[0]["risk_tier"] == "medium"

    def test_low_risk_below_threshold(self, spark):
        from pyspark.sql import Row
        df = spark.createDataFrame([Row(spend_velocity_24h=200.0)])
        result = add_risk_tier(df)
        assert result.collect()[0]["risk_tier"] == "low"


class TestMerchantSegment:

    def test_grocery_maps_to_everyday_spend(self, spark):
        from pyspark.sql import Row
        df     = spark.createDataFrame([Row(merchant_category="grocery")])
        result = add_merchant_segment(df)
        assert result.collect()[0]["merchant_segment"] == "everyday_spend"

    def test_unknown_category_maps_to_other(self, spark):
        from pyspark.sql import Row
        df     = spark.createDataFrame([Row(merchant_category="pawn_shop")])
        result = add_merchant_segment(df)
        assert result.collect()[0]["merchant_segment"] == "other"

    def test_travel_hotel_maps_to_travel(self, spark):
        from pyspark.sql import Row
        df     = spark.createDataFrame([Row(merchant_category="travel_hotel")])
        result = add_merchant_segment(df)
        assert result.collect()[0]["merchant_segment"] == "travel"