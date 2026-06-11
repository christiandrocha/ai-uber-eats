"""Shared fixtures for local PySpark tests (no Databricks connection required)."""
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("ai-uber-eats-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Bronze source schema (mirrors SOURCE_SCHEMA in 01_bronze.ipynb)
# ---------------------------------------------------------------------------
BRONZE_SOURCE_SCHEMA = StructType([
    StructField("event_id",   StringType(), nullable=True),
    StructField("payment_id", StringType(), nullable=True),
    StructField("event", StructType([
        StructField("event_name", StringType(), nullable=True),
        StructField("timestamp",  LongType(),   nullable=True),
    ]), nullable=True),
    StructField("dt_current_timestamp", StringType(), nullable=True),
])

# Full Bronze table schema (after metadata columns are added)
BRONZE_TABLE_SCHEMA = StructType([
    StructField("event_id",              StringType(),   nullable=True),
    StructField("payment_id",            StringType(),   nullable=True),
    StructField("event", StructType([
        StructField("event_name", StringType(), nullable=True),
        StructField("timestamp",  LongType(),   nullable=True),
    ]), nullable=True),
    StructField("dt_current_timestamp",  StringType(),   nullable=True),
    StructField("_ingested_at",          TimestampType(), nullable=True),
    StructField("_ingested_date",        DateType(),     nullable=True),
    StructField("_source_file",          StringType(),   nullable=True),
    StructField("_source_system",        StringType(),   nullable=True),
])

# Silver table schema
SILVER_SCHEMA = StructType([
    StructField("event_id",             StringType(),   nullable=False),
    StructField("payment_id",           StringType(),   nullable=False),
    StructField("event_name",           StringType(),   nullable=False),
    StructField("event_timestamp",      TimestampType(), nullable=False),
    StructField("dt_current_timestamp", TimestampType(), nullable=True),
    StructField("event_date",           DateType(),     nullable=False),
    StructField("_ingested_at",         TimestampType(), nullable=True),
    StructField("_source_file",         StringType(),   nullable=True),
    StructField("_source_system",       StringType(),   nullable=True),
    StructField("_silver_processed_at", TimestampType(), nullable=True),
])

# VALID_EVENT_NAMES and DT_FORMAT are imported from uber_eats.silver above
# and re-exported here so tests that import them from conftest continue to work.

# Epoch ms for 2025-10-05 18:06:40.420 UTC
EPOCH_MS = 1759687600420
EPOCH_TS = datetime(2025, 10, 5, 18, 6, 40, 420000)


@pytest.fixture
def raw_events(spark):
    """Three payment events, one per lifecycle stage, all valid."""
    from pyspark.sql import Row
    rows = [
        Row(
            event_id="ev-001",
            payment_id="pay-A",
            event=Row(event_name="created",    timestamp=EPOCH_MS),
            dt_current_timestamp="2025-10-05 18:06:40.420",
        ),
        Row(
            event_id="ev-002",
            payment_id="pay-A",
            event=Row(event_name="authorized", timestamp=EPOCH_MS + 30_000),
            dt_current_timestamp="2025-10-05 18:07:10.420",
        ),
        Row(
            event_id="ev-003",
            payment_id="pay-A",
            event=Row(event_name="captured",   timestamp=EPOCH_MS + 90_000),
            dt_current_timestamp="2025-10-05 18:08:10.420",
        ),
    ]
    return spark.createDataFrame(rows, schema=BRONZE_SOURCE_SCHEMA)


@pytest.fixture
def raw_events_with_duplicates(spark, raw_events):
    """Same events repeated — simulates double-ingestion."""
    return raw_events.union(raw_events)


@pytest.fixture
def raw_events_with_invalid(spark):
    """Mix of valid and invalid records for quarantine testing."""
    from pyspark.sql import Row
    rows = [
        # valid
        Row(event_id="ev-100", payment_id="pay-B",
            event=Row(event_name="created", timestamp=EPOCH_MS),
            dt_current_timestamp="2025-10-05 18:06:40.420"),
        # null event_id
        Row(event_id=None, payment_id="pay-C",
            event=Row(event_name="created", timestamp=EPOCH_MS),
            dt_current_timestamp="2025-10-05 18:06:40.420"),
        # null payment_id
        Row(event_id="ev-101", payment_id=None,
            event=Row(event_name="created", timestamp=EPOCH_MS),
            dt_current_timestamp="2025-10-05 18:06:40.420"),
        # invalid event_name
        Row(event_id="ev-102", payment_id="pay-D",
            event=Row(event_name="failed", timestamp=EPOCH_MS),
            dt_current_timestamp="2025-10-05 18:06:40.420"),
        # null event.timestamp AND null dt_current_timestamp → no time reference at all
        Row(event_id="ev-103", payment_id="pay-E",
            event=Row(event_name="created", timestamp=None),
            dt_current_timestamp=None),
    ]
    return spark.createDataFrame(rows, schema=BRONZE_SOURCE_SCHEMA)
