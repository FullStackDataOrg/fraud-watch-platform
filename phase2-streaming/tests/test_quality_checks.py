import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../spark_jobs"))

from pyspark.sql import SparkSession
from quality_checks import RULES, split_on_quality


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-quality-checks")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def make_row(**overrides) -> dict:
    base = {
        "transaction_id": "tx-001",
        "user_id": "U0001",
        "amount": 100.0,
        "event_time": 1700000000000,
    }
    return {**base, **overrides}


class TestSplitOnQuality:
    def test_valid_row_goes_to_valid_df(self, spark):
        df = spark.createDataFrame([make_row()])
        valid_df, invalid_df = split_on_quality(df)
        assert valid_df.count() == 1
        assert invalid_df.count() == 0

    def test_null_user_id_rejected(self, spark):
        df = spark.createDataFrame([make_row(user_id=None)])
        valid_df, invalid_df = split_on_quality(df)
        assert valid_df.count() == 0
        assert invalid_df.count() == 1

    def test_null_transaction_id_rejected(self, spark):
        df = spark.createDataFrame([make_row(transaction_id=None)])
        valid_df, invalid_df = split_on_quality(df)
        assert valid_df.count() == 0
        assert invalid_df.count() == 1

    def test_zero_amount_rejected(self, spark):
        df = spark.createDataFrame([make_row(amount=0.0)])
        valid_df, invalid_df = split_on_quality(df)
        assert valid_df.count() == 0
        assert invalid_df.count() == 1

    def test_negative_amount_rejected(self, spark):
        df = spark.createDataFrame([make_row(amount=-5.0)])
        valid_df, invalid_df = split_on_quality(df)
        assert valid_df.count() == 0
        assert invalid_df.count() == 1

    def test_zero_event_time_rejected(self, spark):
        df = spark.createDataFrame([make_row(event_time=0)])
        valid_df, invalid_df = split_on_quality(df)
        assert valid_df.count() == 0
        assert invalid_df.count() == 1

    def test_null_event_time_rejected(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
        schema = StructType([
            StructField("transaction_id", StringType()),
            StructField("user_id",        StringType()),
            StructField("amount",         DoubleType()),
            StructField("event_time",     LongType()),
        ])
        df = spark.createDataFrame([(None, "U001", 100.0, None)], schema=schema)
        valid_df, invalid_df = split_on_quality(df)
        assert invalid_df.count() == 1

    def test_mixed_batch_splits_correctly(self, spark):
        rows = [
            make_row(transaction_id="good-1"),
            make_row(transaction_id="good-2"),
            make_row(user_id=None),
            make_row(amount=-1.0),
        ]
        df = spark.createDataFrame(rows)
        valid_df, invalid_df = split_on_quality(df)
        assert valid_df.count() == 2
        assert invalid_df.count() == 2

    def test_valid_and_invalid_are_disjoint(self, spark):
        rows = [make_row(transaction_id="ok"), make_row(amount=0.0)]
        df = spark.createDataFrame(rows)
        valid_df, invalid_df = split_on_quality(df)
        valid_ids = {r.transaction_id for r in valid_df.collect()}
        invalid_ids = {r.transaction_id for r in invalid_df.collect()}
        assert valid_ids.isdisjoint(invalid_ids)


class TestRulesDefinition:
    def test_all_expected_rules_present(self):
        assert "amount_positive" in RULES
        assert "user_id_not_null" in RULES
        assert "tx_id_not_null" in RULES
        assert "valid_event_time" in RULES

    def test_rules_are_callable(self):
        for name, fn in RULES.items():
            assert callable(fn), f"Rule '{name}' must be a callable"
