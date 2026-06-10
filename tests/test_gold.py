"""Unit tests for the Gold layer aggregation and metrics logic.

Tests run locally with a PySpark local session — no Databricks connection needed.
Logic mirrors 03_gold.ipynb cells, extracted as pure DataFrame transformations.
"""
from datetime import date, datetime

import pytest
from conftest import EPOCH_MS
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from uber_eats.gold import build_gold, gold_merge

# ---------------------------------------------------------------------------
# Silver schema for test fixtures (subset of full Silver schema)
# ---------------------------------------------------------------------------

SILVER_SCHEMA = StructType([
    StructField("event_id",        StringType(),    nullable=False),
    StructField("payment_id",      StringType(),    nullable=False),
    StructField("event_name",      StringType(),    nullable=False),
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("event_date",      DateType(),      nullable=False),
])

EPOCH_S = EPOCH_MS / 1000


# ---------------------------------------------------------------------------
# Silver fixtures for Gold tests
# ---------------------------------------------------------------------------

@pytest.fixture
def silver_full_lifecycle(spark):
    """payment pay-A has all 3 events (created → authorized → captured)."""
    rows = [
        ("ev-001", "pay-A", "created",    datetime(2025, 10, 5, 18, 0, 0), date(2025, 10, 5)),
        ("ev-002", "pay-A", "authorized", datetime(2025, 10, 5, 18, 0, 30), date(2025, 10, 5)),
        ("ev-003", "pay-A", "captured",   datetime(2025, 10, 5, 18, 1, 30), date(2025, 10, 5)),
    ]
    return spark.createDataFrame(rows, schema=SILVER_SCHEMA)


@pytest.fixture
def silver_authorized_only(spark):
    """payment pay-B reached authorization but was not captured."""
    rows = [
        ("ev-004", "pay-B", "created",    datetime(2025, 10, 5, 19, 0, 0), date(2025, 10, 5)),
        ("ev-005", "pay-B", "authorized", datetime(2025, 10, 5, 19, 0, 45), date(2025, 10, 5)),
    ]
    return spark.createDataFrame(rows, schema=SILVER_SCHEMA)


@pytest.fixture
def silver_created_only(spark):
    """payment pay-C only has the created event."""
    rows = [
        ("ev-006", "pay-C", "created", datetime(2025, 10, 5, 20, 0, 0), date(2025, 10, 5)),
    ]
    return spark.createDataFrame(rows, schema=SILVER_SCHEMA)


@pytest.fixture
def silver_multi_payment(spark, silver_full_lifecycle, silver_authorized_only, silver_created_only):
    """Three payments at different lifecycle stages."""
    return (
        silver_full_lifecycle
        .union(silver_authorized_only)
        .union(silver_created_only)
    )


# ---------------------------------------------------------------------------
# One row per payment_id
# ---------------------------------------------------------------------------

class TestGoldRowCardinality:
    def test_one_row_per_payment(self, spark, silver_multi_payment):
        gold = build_gold(silver_multi_payment)
        assert gold.count() == 3

    def test_payment_id_is_unique_in_gold(self, spark, silver_multi_payment):
        gold = build_gold(silver_multi_payment)
        payment_ids = [r["payment_id"] for r in gold.select("payment_id").collect()]
        assert len(payment_ids) == len(set(payment_ids)), "payment_id must be unique in Gold"

    def test_correct_payment_ids_present(self, spark, silver_multi_payment):
        gold = build_gold(silver_multi_payment)
        ids = {r["payment_id"] for r in gold.select("payment_id").collect()}
        assert ids == {"pay-A", "pay-B", "pay-C"}


# ---------------------------------------------------------------------------
# payment_status derivation
# ---------------------------------------------------------------------------

class TestGoldPaymentStatus:
    def _row(self, gold_df, payment_id: str):
        return gold_df.filter(F.col("payment_id") == payment_id).collect()[0]

    def test_captured_status_for_full_lifecycle(self, spark, silver_full_lifecycle):
        gold = build_gold(silver_full_lifecycle)
        row = self._row(gold, "pay-A")
        assert row["payment_status"] == "captured"

    def test_authorized_status_without_capture(self, spark, silver_authorized_only):
        gold = build_gold(silver_authorized_only)
        row = self._row(gold, "pay-B")
        assert row["payment_status"] == "authorized"

    def test_created_status_for_single_event(self, spark, silver_created_only):
        gold = build_gold(silver_created_only)
        row = self._row(gold, "pay-C")
        assert row["payment_status"] == "created"

    def test_captured_takes_priority_over_authorized(self, spark, silver_full_lifecycle):
        """pay-A has all three events; status must be captured (highest priority)."""
        gold = build_gold(silver_full_lifecycle)
        row = self._row(gold, "pay-A")
        assert row["payment_status"] == "captured", (
            "captured must take priority over authorized and created"
        )

    def test_valid_statuses_only(self, spark, silver_multi_payment):
        gold = build_gold(silver_multi_payment)
        statuses = {r["payment_status"] for r in gold.select("payment_status").collect()}
        assert statuses.issubset({"created", "authorized", "captured"})

    def test_payment_captured_without_authorized(self, spark):
        """Edge case: payment goes directly created → captured (skips authorization)."""
        rows = [
            ("ev-x1", "pay-X", "created",  datetime(2025, 10, 5, 9, 0, 0),  date(2025, 10, 5)),
            ("ev-x2", "pay-X", "captured", datetime(2025, 10, 5, 9, 1, 0),  date(2025, 10, 5)),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        gold = build_gold(df)
        row = gold.filter(F.col("payment_id") == "pay-X").collect()[0]
        assert row["payment_status"] == "captured"
        assert row["authorized_at"] is None


# ---------------------------------------------------------------------------
# Lifecycle timestamp columns
# ---------------------------------------------------------------------------

class TestGoldTimestampColumns:
    def _row(self, gold_df, payment_id: str):
        return gold_df.filter(F.col("payment_id") == payment_id).collect()[0]

    def test_created_at_populated(self, spark, silver_full_lifecycle):
        gold = build_gold(silver_full_lifecycle)
        row = self._row(gold, "pay-A")
        assert row["created_at"] is not None
        assert row["created_at"] == datetime(2025, 10, 5, 18, 0, 0)

    def test_authorized_at_populated(self, spark, silver_full_lifecycle):
        gold = build_gold(silver_full_lifecycle)
        row = self._row(gold, "pay-A")
        assert row["authorized_at"] == datetime(2025, 10, 5, 18, 0, 30)

    def test_captured_at_populated(self, spark, silver_full_lifecycle):
        gold = build_gold(silver_full_lifecycle)
        row = self._row(gold, "pay-A")
        assert row["captured_at"] == datetime(2025, 10, 5, 18, 1, 30)

    def test_authorized_at_null_for_created_only(self, spark, silver_created_only):
        gold = build_gold(silver_created_only)
        row = self._row(gold, "pay-C")
        assert row["authorized_at"] is None
        assert row["captured_at"] is None

    def test_captured_at_null_for_authorized_only(self, spark, silver_authorized_only):
        gold = build_gold(silver_authorized_only)
        row = self._row(gold, "pay-B")
        assert row["captured_at"] is None


# ---------------------------------------------------------------------------
# Time metrics
# ---------------------------------------------------------------------------

class TestGoldTimeMetrics:
    def _row(self, gold_df, payment_id: str):
        return gold_df.filter(F.col("payment_id") == payment_id).collect()[0]

    def test_auth_time_30_seconds(self, spark, silver_full_lifecycle):
        """authorized_at - created_at = 30s for pay-A."""
        gold = build_gold(silver_full_lifecycle)
        row = self._row(gold, "pay-A")
        assert row["auth_time_seconds"] == pytest.approx(30.0, abs=0.5)

    def test_capture_time_60_seconds(self, spark, silver_full_lifecycle):
        """captured_at - authorized_at = 60s for pay-A."""
        gold = build_gold(silver_full_lifecycle)
        row = self._row(gold, "pay-A")
        assert row["capture_time_seconds"] == pytest.approx(60.0, abs=0.5)

    def test_total_processing_time_90_seconds(self, spark, silver_full_lifecycle):
        """captured_at - created_at = 90s for pay-A."""
        gold = build_gold(silver_full_lifecycle)
        row = self._row(gold, "pay-A")
        assert row["total_processing_time_seconds"] == pytest.approx(90.0, abs=0.5)

    def test_auth_time_null_when_no_authorized_event(self, spark, silver_created_only):
        gold = build_gold(silver_created_only)
        row = self._row(gold, "pay-C")
        assert row["auth_time_seconds"] is None

    def test_capture_time_null_when_no_captured_event(self, spark, silver_authorized_only):
        gold = build_gold(silver_authorized_only)
        row = self._row(gold, "pay-B")
        assert row["capture_time_seconds"] is None

    def test_total_processing_null_when_not_captured(self, spark, silver_authorized_only):
        gold = build_gold(silver_authorized_only)
        row = self._row(gold, "pay-B")
        assert row["total_processing_time_seconds"] is None

    def test_time_metrics_non_negative(self, spark, silver_multi_payment):
        gold = build_gold(silver_multi_payment)
        for col in ["auth_time_seconds", "capture_time_seconds", "total_processing_time_seconds"]:
            negatives = gold.filter(
                F.col(col).isNotNull() & (F.col(col) < 0)
            ).count()
            assert negatives == 0, f"Negative values found in {col}"


# ---------------------------------------------------------------------------
# Event count
# ---------------------------------------------------------------------------

class TestGoldEventCount:
    def test_event_count_full_lifecycle(self, spark, silver_full_lifecycle):
        gold = build_gold(silver_full_lifecycle)
        row = gold.filter(F.col("payment_id") == "pay-A").collect()[0]
        assert row["event_count"] == 3

    def test_event_count_authorized_only(self, spark, silver_authorized_only):
        gold = build_gold(silver_authorized_only)
        row = gold.filter(F.col("payment_id") == "pay-B").collect()[0]
        assert row["event_count"] == 2

    def test_event_count_created_only(self, spark, silver_created_only):
        gold = build_gold(silver_created_only)
        row = gold.filter(F.col("payment_id") == "pay-C").collect()[0]
        assert row["event_count"] == 1


# ---------------------------------------------------------------------------
# Gold quality checks (mirrors cell-6)
# ---------------------------------------------------------------------------

class TestGoldQualityChecks:
    def test_no_null_payment_ids(self, spark, silver_multi_payment):
        gold = build_gold(silver_multi_payment)
        null_count = gold.filter(F.col("payment_id").isNull()).count()
        assert null_count == 0

    def test_no_invalid_statuses(self, spark, silver_multi_payment):
        gold = build_gold(silver_multi_payment)
        invalid = gold.filter(
            ~F.col("payment_status").isin(["created", "authorized", "captured"])
        ).count()
        assert invalid == 0

    def test_max_picks_latest_event_timestamp(self, spark):
        """When a payment_id has two 'created' events, max() picks the later one."""
        rows = [
            ("ev-d1", "pay-D", "created", datetime(2025, 10, 5, 8, 0, 0), date(2025, 10, 5)),
            ("ev-d2", "pay-D", "created", datetime(2025, 10, 5, 9, 0, 0), date(2025, 10, 5)),
        ]
        df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
        gold = build_gold(df)
        row = gold.filter(F.col("payment_id") == "pay-D").collect()[0]
        assert row["created_at"] == datetime(2025, 10, 5, 9, 0, 0), (
            "max() must pick the latest created event"
        )
        assert row["event_count"] == 2


# ---------------------------------------------------------------------------
# Idempotency / MERGE
# ---------------------------------------------------------------------------

class TestGoldIdempotency:
    def test_first_run_inserts_all(self, spark, silver_multi_payment):
        gold = build_gold(silver_multi_payment)
        empty = spark.createDataFrame([], schema=gold.schema)
        result = gold_merge(empty, gold)
        assert result.count() == 3

    def test_rerun_same_data_no_change(self, spark, silver_multi_payment):
        gold = build_gold(silver_multi_payment)
        result = gold_merge(gold, gold)
        assert result.count() == gold.count(), "Re-running Gold merge must be idempotent"

    def test_new_payment_added_on_incremental_run(self, spark, silver_multi_payment):
        gold_v1 = build_gold(silver_multi_payment)

        extra_rows = [
            ("ev-100", "pay-NEW", "captured", datetime(2025, 10, 6, 10, 0, 0), date(2025, 10, 6)),
        ]
        extra_silver = spark.createDataFrame(extra_rows, schema=SILVER_SCHEMA)
        gold_v2 = build_gold(silver_multi_payment.union(extra_silver))

        result = gold_merge(gold_v1, gold_v2)
        assert result.count() == 4

    def test_payment_status_updated_on_lifecycle_progression(self, spark, silver_created_only):
        """pay-C starts as 'created', then progresses to 'captured' in a later run."""
        gold_v1 = build_gold(silver_created_only)
        row_v1 = gold_v1.filter(F.col("payment_id") == "pay-C").collect()[0]
        assert row_v1["payment_status"] == "created"

        # New Silver data: pay-C now also has authorized + captured events
        new_rows = [
            ("ev-006", "pay-C", "created",    datetime(2025, 10, 5, 20, 0, 0),  date(2025, 10, 5)),
            ("ev-007", "pay-C", "authorized", datetime(2025, 10, 5, 20, 0, 30), date(2025, 10, 5)),
            ("ev-008", "pay-C", "captured",   datetime(2025, 10, 5, 20, 1, 30), date(2025, 10, 5)),
        ]
        silver_v2 = spark.createDataFrame(new_rows, schema=SILVER_SCHEMA)
        gold_v2 = build_gold(silver_v2)

        result = gold_merge(gold_v1, gold_v2)
        row_v2 = result.filter(F.col("payment_id") == "pay-C").collect()[0]
        assert row_v2["payment_status"] == "captured", (
            "Gold MERGE must update payment_status as lifecycle progresses"
        )
