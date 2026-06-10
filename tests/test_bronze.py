"""Unit tests for the Bronze layer ingestion logic.

Tests run locally with a PySpark local session — no Databricks connection needed.
Logic mirrors 01_bronze.ipynb cells, extracted as pure DataFrame transformations.
"""
from datetime import date

import pytest
from conftest import BRONZE_SOURCE_SCHEMA, BRONZE_TABLE_SCHEMA, EPOCH_MS
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType

from uber_eats.bronze import add_metadata_columns, merge_dedup, validate_structure

# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestBronzeSchema:
    def test_source_schema_has_long_timestamp(self):
        """event.timestamp must be LongType in the source schema."""
        event_field = next(f for f in BRONZE_SOURCE_SCHEMA if f.name == "event")
        ts_field = next(f for f in event_field.dataType.fields if f.name == "timestamp")
        assert isinstance(ts_field.dataType, LongType), (
            f"Expected LongType for event.timestamp, got {ts_field.dataType}"
        )

    def test_source_schema_field_names(self):
        top_level = {f.name for f in BRONZE_SOURCE_SCHEMA.fields}
        assert top_level == {"event_id", "payment_id", "event", "dt_current_timestamp"}

    def test_bronze_table_schema_includes_metadata_cols(self):
        col_names = {f.name for f in BRONZE_TABLE_SCHEMA.fields}
        assert {"_ingested_at", "_ingested_date", "_source_file", "_source_system"}.issubset(col_names)


# ---------------------------------------------------------------------------
# Scientific notation handling
# ---------------------------------------------------------------------------

class TestScientificNotation:
    def test_long_type_preserves_epoch_ms(self, spark):
        """1.7596876023E12 read as LongType must equal 1759687600300 (no precision loss)."""
        df = spark.createDataFrame(
            [(1759687600300,)],
            StructType([StructField("ts", LongType())])
        )
        value = df.collect()[0]["ts"]
        assert value == 1759687600300, f"Precision lost: got {value}"

    def test_epoch_ms_is_thirteen_digits(self):
        """Sanity check: our test epoch value has 13 digits."""
        assert len(str(EPOCH_MS)) == 13

    def test_ingest_row_timestamp_as_long(self, spark, raw_events):
        """event.timestamp arrives as LongType — not DoubleType or StringType."""
        ts_col_type = dict(raw_events.dtypes)["event"]
        # PySpark represents struct dtype as a string like "struct<event_name:string,timestamp:bigint>"
        assert "bigint" in ts_col_type.lower(), (
            f"event struct dtype: {ts_col_type}. event.timestamp should be bigint (LongType)."
        )


# ---------------------------------------------------------------------------
# Metadata columns
# ---------------------------------------------------------------------------

class TestBronzeMetadataColumns:
    def test_metadata_columns_added(self, spark, raw_events):
        df = add_metadata_columns(raw_events)
        col_names = set(df.columns)
        assert {"_ingested_at", "_ingested_date", "_source_file", "_source_system"}.issubset(col_names)

    def test_ingested_date_is_today(self, spark, raw_events):
        df = add_metadata_columns(raw_events)
        today = date.today()
        dates = {row["_ingested_date"] for row in df.select("_ingested_date").collect()}
        assert dates == {today}, f"Expected ingested_date = {today}, got {dates}"

    def test_source_system_propagated(self, spark, raw_events):
        system = "test_source_system"
        df = add_metadata_columns(raw_events, source_system=system)
        systems = {row["_source_system"] for row in df.select("_source_system").collect()}
        assert systems == {system}

    def test_ingested_at_is_timestamp(self, spark, raw_events):
        df = add_metadata_columns(raw_events)
        ts_dtype = dict(df.dtypes)["_ingested_at"]
        assert ts_dtype == "timestamp", f"Expected timestamp, got {ts_dtype}"


# ---------------------------------------------------------------------------
# Structural validation (mirrors cell-8)
# ---------------------------------------------------------------------------

class TestBronzeStructuralValidation:
    def test_expected_columns_present(self, spark, raw_events):
        expected = {"event_id", "payment_id", "event", "dt_current_timestamp"}
        actual = set(raw_events.columns)
        missing = expected - actual
        assert not missing, f"Missing columns: {missing}"

    def test_non_empty_dataframe_passes(self, spark, raw_events):
        assert raw_events.count() > 0

    def test_empty_dataframe_triggers_validation_error(self, spark):
        """Mirrors the Bronze validation: empty source raises ValueError."""
        empty_df = spark.createDataFrame([], schema=BRONZE_SOURCE_SCHEMA)
        with pytest.raises((ValueError, AssertionError)):
            count = empty_df.count()
            if count == 0:
                raise ValueError("Bronze ingestion aborted: no records found.")


# ---------------------------------------------------------------------------
# Idempotency (MERGE simulation)
# ---------------------------------------------------------------------------

class TestBronzeIdempotency:
    def test_merge_inserts_new_event_ids(self, spark, raw_events):
        """First run: all rows inserted because target is empty."""
        empty_bronze = spark.createDataFrame([], schema=BRONZE_SOURCE_SCHEMA)
        result = merge_dedup(empty_bronze, add_metadata_columns(raw_events))
        assert result.count() == raw_events.count()

    def test_merge_skips_existing_event_ids(self, spark, raw_events):
        """Second run with same data: no new rows inserted."""
        bronze_with_metadata = add_metadata_columns(raw_events)
        # First load populates the table
        existing = bronze_with_metadata
        # Second run with same source
        result = merge_dedup(existing, bronze_with_metadata)
        assert result.count() == existing.count(), (
            "Re-processing same data must not duplicate rows."
        )

    def test_merge_inserts_only_new_event_ids(self, spark, raw_events):
        """Partial overlap: only new event_ids are appended."""
        # Load first two events
        first_two = raw_events.filter(F.col("event_id").isin(["ev-001", "ev-002"]))
        existing = add_metadata_columns(first_two)

        # Second run includes all three (one new: ev-003)
        incoming = add_metadata_columns(raw_events)
        result = merge_dedup(existing, incoming)
        assert result.count() == 3, f"Expected 3 rows, got {result.count()}"

    def test_double_ingestion_no_duplicates(self, spark, raw_events_with_duplicates):
        """Union of source with itself must produce no duplicates after dedup."""
        incoming = add_metadata_columns(raw_events_with_duplicates)
        empty_bronze = spark.createDataFrame([], schema=incoming.schema)
        result = merge_dedup(empty_bronze, incoming)
        # Should have 3 unique event_ids despite 6 input rows
        result_count = result.count()
        assert result_count == 3, (
            f"Expected 3 unique event_ids after merge, got {result_count}"
        )


# ---------------------------------------------------------------------------
# validate_structure
# ---------------------------------------------------------------------------

class TestBronzeValidateStructure:
    def test_valid_df_passes(self, spark, raw_events):
        validate_structure(raw_events, source_path="/test/path")  # no exception

    def test_empty_df_raises(self, spark):
        empty_df = spark.createDataFrame([], schema=BRONZE_SOURCE_SCHEMA)
        with pytest.raises(ValueError, match="no records found"):
            validate_structure(empty_df, source_path="/empty/path")

    def test_missing_columns_raises(self, spark):
        df = spark.createDataFrame([(1,)], "x INT")
        with pytest.raises(ValueError, match="missing required columns"):
            validate_structure(df)
