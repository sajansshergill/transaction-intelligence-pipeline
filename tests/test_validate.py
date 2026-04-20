"""
tests/test_validate.py — Unit tests for the PySpark validation job.
Uses a local SparkSession (no cluster required).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, BooleanType, StructType, StructField
from pyspark.errors.exceptions.base import PySparkRuntimeError

from processing.jobs.validate import (
    enforce_schema,
    build_validation_flags,
    split_valid_invalid,
    VALID_ACCOUNT_TYPES,
    VALID_TXN_TYPES,
    VALID_CHANNELS,
)


@pytest.fixture(scope="module")
def spark():
    try:
        return (
            SparkSession.builder
            .master("local[1]")
            .appName("test_validate")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except PySparkRuntimeError as e:
        pytest.skip(f"Skipping Spark tests (Spark JVM failed to start): {e}")


def _make_record(**overrides):
    base = {
        "transaction_id":    "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "customer_id":       "CUST_ABC123",
        "merchant_id":       "MER_XYZ999",
        "merchant_name":     "Test Merchant",
        "merchant_category": "grocery",
        "city":              "New York",
        "state":             "NY",
        "amount_usd":        42.50,
        "currency":          "USD",
        "account_type":      "checking",
        "transaction_type":  "purchase",
        "channel":           "in_store",
        "event_time":        "2025-04-12T14:30:00+00:00",
        "kafka_ingest_time": "2025-04-12T14:30:01+00:00",
        "is_international":  False,
        "zip_code":          "10001",
    }
    base.update(overrides)
    return base


class TestEnforceSchema:

    def test_amount_cast_to_double(self, spark):
        row    = _make_record(amount_usd=99.99)
        df     = spark.createDataFrame([row])
        result = enforce_schema(df)
        assert dict(result.dtypes)["amount_usd"] == "double"

    def test_event_time_ts_parsed(self, spark):
        row    = _make_record()
        df     = spark.createDataFrame([row])
        result = enforce_schema(df)
        assert "event_time_ts" in result.columns
        assert dict(result.dtypes)["event_time_ts"] == "timestamp"

    def test_ingest_date_derived(self, spark):
        row    = _make_record()
        df     = spark.createDataFrame([row])
        result = enforce_schema(df)
        assert "ingest_date" in result.columns


class TestValidationFlags:

    def test_valid_row_passes_all_checks(self, spark):
        row     = _make_record()
        df      = spark.createDataFrame([row])
        typed   = enforce_schema(df)
        flagged = build_validation_flags(typed)
        check_cols = [c for c in flagged.columns if c.startswith("_chk_")]
        row_out = flagged.collect()[0]
        for col in check_cols:
            assert row_out[col] is True, f"Expected {col} to be True"

    def test_null_transaction_id_fails(self, spark):
        row     = _make_record(transaction_id=None)
        df      = spark.createDataFrame([row])
        typed   = enforce_schema(df)
        flagged = build_validation_flags(typed)
        row_out = flagged.collect()[0]
        assert row_out["_chk_txn_id_notnull"] is False

    def test_negative_amount_fails(self, spark):
        row     = _make_record(amount_usd=-5.00)
        df      = spark.createDataFrame([row])
        typed   = enforce_schema(df)
        flagged = build_validation_flags(typed)
        row_out = flagged.collect()[0]
        assert row_out["_chk_amount_range"] is False

    def test_amount_over_max_fails(self, spark):
        row     = _make_record(amount_usd=99_999.99)
        df      = spark.createDataFrame([row])
        typed   = enforce_schema(df)
        flagged = build_validation_flags(typed)
        row_out = flagged.collect()[0]
        assert row_out["_chk_amount_range"] is False

    def test_invalid_account_type_fails(self, spark):
        row     = _make_record(account_type="savings")   # not in enum
        df      = spark.createDataFrame([row])
        typed   = enforce_schema(df)
        flagged = build_validation_flags(typed)
        row_out = flagged.collect()[0]
        assert row_out["_chk_account_type"] is False

    def test_invalid_channel_fails(self, spark):
        row     = _make_record(channel="drive_through")
        df      = spark.createDataFrame([row])
        typed   = enforce_schema(df)
        flagged = build_validation_flags(typed)
        row_out = flagged.collect()[0]
        assert row_out["_chk_channel"] is False

    def test_malformed_uuid_fails(self, spark):
        row     = _make_record(transaction_id="not-a-uuid")
        df      = spark.createDataFrame([row])
        typed   = enforce_schema(df)
        flagged = build_validation_flags(typed)
        row_out = flagged.collect()[0]
        assert row_out["_chk_txn_id_format"] is False


class TestSplitValidInvalid:

    def test_valid_row_goes_to_valid_df(self, spark):
        row     = _make_record()
        df      = spark.createDataFrame([row])
        typed   = enforce_schema(df)
        flagged = build_validation_flags(typed)
        valid, invalid = split_valid_invalid(flagged)
        assert valid.count()   == 1
        assert invalid.count() == 0

    def test_invalid_row_goes_to_invalid_df(self, spark):
        row     = _make_record(amount_usd=-1.00, account_type="savings")
        df      = spark.createDataFrame([row])
        typed   = enforce_schema(df)
        flagged = build_validation_flags(typed)
        valid, invalid = split_valid_invalid(flagged)
        assert valid.count()   == 0
        assert invalid.count() == 1

    def test_mixed_batch_split_correctly(self, spark):
        rows = [
            _make_record(transaction_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
            _make_record(transaction_id="b2c3d4e5-f6a7-8901-bcde-f12345678901"),
            _make_record(amount_usd=-5.00),   # invalid
        ]
        df      = spark.createDataFrame(rows)
        typed   = enforce_schema(df)
        flagged = build_validation_flags(typed)
        valid, invalid = split_valid_invalid(flagged)
        assert valid.count()   == 2
        assert invalid.count() == 1