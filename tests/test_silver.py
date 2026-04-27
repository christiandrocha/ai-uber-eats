"""Unit tests for the Silver layer transformations.

Tests run locally with a PySpark local session — no Databricks connection needed.
Logic mirrors 02_silver.ipynb cells, extracted as pure DataFrame transformations.
"""
import pytest
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, TimestampType

from conftest import (
    BRONZE_SOURCE_SCHEMA,
    VALID_EVENT_NAMES,
    DT_FORMAT,
    EPOCH_MS,
)


# ---------------------------------------------------------------------------
# Helpers mirroring 02_silver.ipynb cells
# ---------------------------------------------------------------------------

def apply_quality_gate(df):
    """Mirrors cell-6: classify each row as valid or quarantine."""
    valid_names_list = list(VALID_EVENT_NAMES)
    return df.withColumn(
        "_quarantine_reason",
        F.when(F.col("event_id").isNull(),           F.lit("event_id is null"))
         .when(F.col("payment_id").isNull(),          F.lit("payment_id is null"))
         .when(F.col("event").isNull(),               F.lit("event struct is null"))
         .when(
             ~F.col("event.event_name").isin(valid_names_list),
             F.concat(
                 F.lit("invalid event_name: "),
                 F.coalesce(F.col("event.event_name"), F.lit("<null>"))
             )
         )
         .when(F.col("event.timestamp").isNull(),     F.lit("event.timestamp is null"))
         .otherwise(F.lit(None).cast(StringType()))
    )


def apply_transformations(valid_df):
    """Mirrors cell-8: type casts, struct flattening, metadata."""
    return (
        valid_df
        .withColumn("event_name",   F.col("event.event_name"))
        .withColumn(
            "event_timestamp",
            F.timestamp_seconds((F.col("event.timestamp") / 1000).cast("double"))
        )
        .withColumn(
            "dt_current_timestamp",
            F.to_timestamp(F.col("dt_current_timestamp"), DT_FORMAT)
        )
        .withColumn("event_date",           F.to_date(F.col("event_timestamp")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("event")
        .select(
            "event_id",
            "payment_id",
            "event_name",
            "event_timestamp",
            "dt_current_timestamp",
            "event_date",
            "_silver_processed_at",
        )
    )


def deduplicate(df, business_key: str = "event_id"):
    """Mirrors cell-9: keep the most-recently-ingested row per business_key."""
    # Add a deterministic ordering column when _ingested_at isn't present (test mode)
    if "_ingested_at" not in df.columns:
        df = df.withColumn("_ingested_at", F.current_timestamp())

    window = Window.partitionBy(business_key).orderBy(F.col("_ingested_at").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def silver_merge_dedup(existing_df, incoming_df, business_key: str = "event_id"):
    """
    Simulates the Silver MERGE:
      - WHEN MATCHED AND source._ingested_at > target._ingested_at → UPDATE
      - WHEN NOT MATCHED → INSERT
    """
    if existing_df.rdd.isEmpty():
        return incoming_df

    # Rows that already exist in Silver → keep newer version
    matched = (
        incoming_df.alias("s")
        .join(existing_df.alias("t"), business_key)
        .where(F.col("s._silver_processed_at") > F.col("t._silver_processed_at"))
        .select("s.*")
    )
    # Rows that don't exist in Silver yet
    not_matched = incoming_df.join(
        existing_df.select(business_key),
        on=business_key,
        how="left_anti",
    )
    # Keep existing rows that were NOT updated, plus matched updates, plus new inserts
    untouched_existing = existing_df.join(
        matched.select(business_key),
        on=business_key,
        how="left_anti",
    )
    return untouched_existing.unionByName(matched).unionByName(not_matched)


# ---------------------------------------------------------------------------
# Quality gate tests
# ---------------------------------------------------------------------------

class TestSilverQualityGate:
    def test_valid_records_pass_gate(self, spark, raw_events):
        checked = apply_quality_gate(raw_events)
        valid = checked.filter(F.col("_quarantine_reason").isNull())
        assert valid.count() == raw_events.count()

    def test_null_event_id_quarantined(self, spark, raw_events_with_invalid):
        checked = apply_quality_gate(raw_events_with_invalid)
        q = checked.filter(F.col("_quarantine_reason") == "event_id is null")
        assert q.count() == 1, "Expected exactly one row with null event_id"

    def test_null_payment_id_quarantined(self, spark, raw_events_with_invalid):
        checked = apply_quality_gate(raw_events_with_invalid)
        q = checked.filter(F.col("_quarantine_reason") == "payment_id is null")
        assert q.count() == 1

    def test_invalid_event_name_quarantined(self, spark, raw_events_with_invalid):
        checked = apply_quality_gate(raw_events_with_invalid)
        q = checked.filter(F.col("_quarantine_reason").startswith("invalid event_name:"))
        assert q.count() == 1
        reason = q.collect()[0]["_quarantine_reason"]
        assert "refunded" in reason

    def test_null_timestamp_quarantined(self, spark, raw_events_with_invalid):
        checked = apply_quality_gate(raw_events_with_invalid)
        q = checked.filter(F.col("_quarantine_reason") == "event.timestamp is null")
        assert q.count() == 1

    def test_quarantine_count_correct(self, spark, raw_events_with_invalid):
        """4 invalid + 1 valid → 4 quarantined, 1 valid."""
        checked = apply_quality_gate(raw_events_with_invalid)
        valid_count = checked.filter(F.col("_quarantine_reason").isNull()).count()
        quarantine_count = checked.filter(F.col("_quarantine_reason").isNotNull()).count()
        assert valid_count == 1
        assert quarantine_count == 4

    def test_first_failing_rule_wins(self, spark):
        """Row with null event_id AND null payment_id → first rule (event_id is null)."""
        from pyspark.sql import Row
        row = Row(event_id=None, payment_id=None,
                  event=Row(event_name="created", timestamp=EPOCH_MS),
                  dt_current_timestamp="2025-10-05 18:06:40.420")
        df = spark.createDataFrame([row], schema=BRONZE_SOURCE_SCHEMA)
        checked = apply_quality_gate(df)
        reason = checked.collect()[0]["_quarantine_reason"]
        assert reason == "event_id is null"


# ---------------------------------------------------------------------------
# Timestamp transformation tests
# ---------------------------------------------------------------------------

class TestSilverTimestampTransformations:
    def test_epoch_ms_converted_to_timestamp(self, spark, raw_events):
        checked = apply_quality_gate(raw_events)
        valid = checked.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        transformed = apply_transformations(valid)

        rows = {r["event_id"]: r for r in transformed.collect()}
        ts = rows["ev-001"]["event_timestamp"]
        # Compare via epoch to be timezone-independent:
        # Spark stores timestamps as UTC internally and returns local-time naive datetimes.
        # ts.timestamp() converts the local-naive datetime back to UTC epoch seconds.
        ts_epoch_ms = round(ts.timestamp() * 1000)
        assert ts_epoch_ms == EPOCH_MS, (
            f"Expected epoch ms {EPOCH_MS}, got {ts_epoch_ms}. "
            "Precision loss or incorrect ms→s conversion."
        )

    def test_dt_current_timestamp_parsed(self, spark, raw_events):
        checked = apply_quality_gate(raw_events)
        valid = checked.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        transformed = apply_transformations(valid)

        row = transformed.filter(F.col("event_id") == "ev-001").collect()[0]
        dt = row["dt_current_timestamp"]
        assert isinstance(dt, datetime)
        assert dt.year == 2025

    def test_event_date_derived_from_event_timestamp(self, spark, raw_events):
        checked = apply_quality_gate(raw_events)
        valid = checked.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        transformed = apply_transformations(valid)

        row = transformed.filter(F.col("event_id") == "ev-001").collect()[0]
        assert row["event_date"] == date(2025, 10, 5)

    def test_event_struct_dropped_in_silver(self, spark, raw_events):
        checked = apply_quality_gate(raw_events)
        valid = checked.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        transformed = apply_transformations(valid)
        assert "event" not in transformed.columns, "Silver must not contain the raw event struct"

    def test_event_name_flattened_to_scalar(self, spark, raw_events):
        checked = apply_quality_gate(raw_events)
        valid = checked.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        transformed = apply_transformations(valid)
        assert "event_name" in transformed.columns
        names = {r["event_name"] for r in transformed.select("event_name").collect()}
        assert names == {"created", "authorized", "captured"}

    def test_ms_offset_preserved(self, spark):
        """30-second offset between ev-001 and ev-002 must be reflected in event_timestamp."""
        from pyspark.sql import Row
        rows = [
            Row(event_id="ev-001", payment_id="pay-A",
                event=Row(event_name="created",    timestamp=EPOCH_MS),
                dt_current_timestamp="2025-10-05 18:06:40.420"),
            Row(event_id="ev-002", payment_id="pay-A",
                event=Row(event_name="authorized", timestamp=EPOCH_MS + 30_000),
                dt_current_timestamp="2025-10-05 18:07:10.420"),
        ]
        df = spark.createDataFrame(rows, schema=BRONZE_SOURCE_SCHEMA)
        checked = apply_quality_gate(df)
        valid = checked.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        transformed = apply_transformations(valid)

        result = {r["event_id"]: r["event_timestamp"] for r in transformed.collect()}
        delta = (result["ev-002"] - result["ev-001"]).total_seconds()
        assert delta == pytest.approx(30.0, abs=0.1), f"Expected 30s delta, got {delta}"


# ---------------------------------------------------------------------------
# Deduplication tests
# ---------------------------------------------------------------------------

class TestSilverDeduplication:
    def test_dedup_removes_duplicate_event_ids(self, spark, raw_events_with_duplicates):
        checked = apply_quality_gate(raw_events_with_duplicates)
        valid = checked.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        transformed = apply_transformations(valid)
        deduped = deduplicate(transformed)
        assert deduped.count() == 3, "Expected 3 unique event_ids after dedup"

    def test_dedup_selects_latest_ingested(self, spark):
        """When two rows share an event_id, the one with the latest _ingested_at is kept."""
        from pyspark.sql import Row
        rows = [
            Row(event_id="ev-001", payment_id="pay-A",
                event_name="created",
                event_timestamp=datetime(2025, 10, 5, 18, 0, 0),
                dt_current_timestamp=datetime(2025, 10, 5, 18, 0, 0),
                event_date=date(2025, 10, 5),
                _ingested_at=datetime(2025, 10, 5, 12, 0, 0),
                _silver_processed_at=datetime(2025, 10, 5, 12, 0, 0)),
            Row(event_id="ev-001", payment_id="pay-A",
                event_name="created",
                event_timestamp=datetime(2025, 10, 5, 18, 0, 0),
                dt_current_timestamp=datetime(2025, 10, 5, 18, 0, 0),
                event_date=date(2025, 10, 5),
                _ingested_at=datetime(2025, 10, 5, 13, 0, 0),  # later
                _silver_processed_at=datetime(2025, 10, 5, 13, 0, 0)),
        ]
        df = spark.createDataFrame(rows)
        deduped = deduplicate(df)
        assert deduped.count() == 1
        kept = deduped.collect()[0]
        assert kept["_ingested_at"].hour == 13, "Must keep the later-ingested row"

    def test_dedup_is_deterministic(self, spark, raw_events):
        """Running dedup twice on the same data yields the same result for stable columns.

        Metadata columns (_silver_processed_at, _ingested_at) use current_timestamp()
        and re-evaluate on each action — excluded from comparison intentionally.
        """
        checked = apply_quality_gate(raw_events)
        valid = checked.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        transformed = apply_transformations(valid)
        stable = ["event_id", "payment_id", "event_name", "event_timestamp", "event_date"]
        run1 = deduplicate(transformed).orderBy("event_id").select(*stable).collect()
        run2 = deduplicate(transformed).orderBy("event_id").select(*stable).collect()
        assert run1 == run2


# ---------------------------------------------------------------------------
# Idempotency / MERGE tests
# ---------------------------------------------------------------------------

class TestSilverIdempotency:
    def _build_silver(self, spark, raw_events):
        checked = apply_quality_gate(raw_events)
        valid = checked.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
        return deduplicate(apply_transformations(valid))

    def test_first_run_inserts_all(self, spark, raw_events):
        silver = self._build_silver(spark, raw_events)
        empty = spark.createDataFrame([], schema=silver.schema)
        result = silver_merge_dedup(empty, silver)
        assert result.count() == 3

    def test_rerun_with_same_data_no_change(self, spark, raw_events):
        silver = self._build_silver(spark, raw_events)
        result = silver_merge_dedup(silver, silver)
        assert result.count() == silver.count(), "Re-run must be idempotent"

    def test_new_event_inserted_on_rerun(self, spark, raw_events):
        """New event_id in second run must be added to Silver."""
        from pyspark.sql import Row
        initial = self._build_silver(spark, raw_events)

        # _ingested_at is added by deduplicate() — must match initial.schema (8 fields)
        extra_row = Row(
            event_id="ev-999",
            payment_id="pay-Z",
            event_name="created",
            event_timestamp=datetime(2025, 10, 6, 0, 0, 0),
            dt_current_timestamp=datetime(2025, 10, 6, 0, 0, 0),
            event_date=date(2025, 10, 6),
            _silver_processed_at=datetime(2025, 10, 6, 1, 0, 0),
            _ingested_at=datetime(2025, 10, 6, 0, 30, 0),
        )
        extra_df = spark.createDataFrame([extra_row], schema=initial.schema)
        second_run = initial.unionByName(extra_df)
        result = silver_merge_dedup(initial, second_run)
        assert result.count() == 4
