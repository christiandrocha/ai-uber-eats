# ai-uber-eats

> Medallion data pipeline for Uber Eats payment events — Bronze → Silver → Gold on Databricks Serverless with Unity Catalog.

## Overview

This project implements a production-grade **Medallion Architecture** pipeline that ingests 100 JSON files containing Uber Eats payment events, cleanses and enriches the data through layered transformations, and delivers a business-ready payment lifecycle summary to the Gold layer.

The pipeline runs as a Databricks Job (3 sequential notebook tasks) deployed via Databricks Asset Bundles (DABs). All compute runs on **Databricks Serverless** using the Spark Connect API — no classic clusters, no RDDs.

---

## Architecture

```
/Volumes/aiubereats/payments/raw_json/
           │  100 × JSON files
           ▼
┌──────────────────────┐
│   Bronze Layer       │  Raw ingestion, idempotent MERGE on event_id
│   bronze_payment_    │  Explicit schema, PERMISSIVE mode, partition by _ingested_date
│   events             │  Source fidelity preserved — no transformation
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Silver Layer       │  Cleansed, typed, deduplicated, enum-validated
│   silver_payment_    │  Quarantine for rejected rows, MERGE upsert on event_id
│   events             │  Liquid Clustering on (event_date, payment_id, event_name)
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Gold Layer         │  One row per payment_id — full lifecycle summary
│   gold_payment_      │  Payment status, timing metrics, MERGE on payment_id
│   summary            │  Liquid Clustering on (payment_status, payment_id)
└──────────────────────┘
```

### Bronze — Raw Ingestion

**Table**: `aiubereats.payments.bronze_payment_events`

Reads JSON files from the Unity Catalog Volume with an explicit `StructType` schema (prevents `event.timestamp` from being inferred as `DoubleType` due to scientific notation such as `1.7596876023E12`). Adds ingestion metadata columns and performs a `MERGE` on `event_id` for idempotent re-runs. Partitioned by `_ingested_date`.

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING | Unique event identifier (merge key) |
| `payment_id` | STRING | Payment entity foreign key |
| `event` | STRUCT | Nested struct: `event_name` (STRING), `timestamp` (BIGINT epoch ms) |
| `dt_current_timestamp` | STRING | Source-provided timestamp string |
| `_ingested_at` | TIMESTAMP | Batch ingestion wall-clock time |
| `_ingested_date` | DATE | Partition column derived from `_ingested_at` |
| `_source_file` | STRING | Full path of the originating JSON file |
| `_source_system` | STRING | Logical system identifier for lineage |

### Silver — Cleansed & Conformed

**Table**: `aiubereats.payments.silver_payment_events`

Applies 7 transformations: deduplication on `event_id` (keeps latest `_ingested_at`), type casting (`BIGINT` epoch ms → `TIMESTAMP`, source string → `TIMESTAMP`), struct flattening, null rejection, `event_name` enum validation, and a `MERGE` upsert into Silver. Rows that fail quality gates are written to `aiubereats.payments.quarantine_payment_events` with a `_quarantine_reason` label.

Valid `event_name` values: `created`, `authorized`, `captured`.

### Gold — Payment Lifecycle Summary

**Table**: `aiubereats.payments.gold_payment_summary`

One row per `payment_id`. Pivots events via conditional aggregation (`max(CASE WHEN event_name = '...')`) to derive lifecycle timestamps and three timing metrics.

| Column | Type | Description |
|--------|------|-------------|
| `payment_id` | STRING | Business key |
| `created_at` | TIMESTAMP | Timestamp of the `created` event |
| `authorized_at` | TIMESTAMP | Timestamp of the `authorized` event |
| `captured_at` | TIMESTAMP | Timestamp of the `captured` event |
| `payment_status` | STRING | `captured` / `authorized` / `created` |
| `auth_time_seconds` | DOUBLE | `authorized_at − created_at` (null if not authorized) |
| `capture_time_seconds` | DOUBLE | `captured_at − authorized_at` (null if not captured) |
| `total_processing_time_seconds` | DOUBLE | `captured_at − created_at` (null if not captured) |
| `event_count` | LONG | Distinct events observed for this payment |
| `_computed_at` | TIMESTAMP | When this Gold row was last recomputed |

---

## Source Data Schema

Each JSON file contains one payment event per record:

```json
{
  "event_id": "evt_abc123",
  "payment_id": "pay_xyz456",
  "event": {
    "event_name": "captured",
    "timestamp": 1759687602300
  },
  "dt_current_timestamp": "2025-10-05 18:06:40.420"
}
```

`event.timestamp` may arrive in scientific notation (`1.7596876023E12`). The explicit Bronze schema forces `LongType` coercion, which Spark's JSON reader handles correctly — see the sanity-check cell at the end of `01_bronze.ipynb`.

---

## Repository Structure

```
ai-uber-eats/
├── databricks.yml              # Databricks Asset Bundle — job definition & targets
├── notebooks/
│   ├── 01_bronze.ipynb         # Raw ingestion
│   ├── 02_silver.ipynb         # Cleanse, deduplicate, quarantine
│   └── 03_gold.ipynb           # Payment lifecycle aggregation
├── src/
│   └── uber_eats/
│       └── __init__.py
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
└── README.md
```

---

## Prerequisites

| Requirement | Version / Notes |
|-------------|----------------|
| Databricks workspace | Free Edition (Serverless) |
| Databricks CLI | `v0.200+` — install via `pip install databricks-cli` or `brew install databricks` |
| Python | 3.10+ |
| Unity Catalog | Catalog `aiubereats`, schema `payments`, volume `raw_json` must exist or be created by the notebooks |
| JSON source files | 100 files uploaded to `/Volumes/aiubereats/payments/raw_json/` |

---

## Environment Setup

### 1. Clone the repository

```bash
git clone https://github.com/christiandrocha/ai-uber-eats.git
cd ai-uber-eats
```

### 2. Configure the Databricks CLI

```bash
databricks configure --host https://dbc-f3701868-1581.cloud.databricks.com
# Enter your personal access token when prompted
```

Verify the connection:

```bash
databricks workspace list /
```

### 3. Upload source JSON files

Place your 100 JSON payment-event files in the Unity Catalog Volume:

```bash
databricks fs cp --recursive ./data/ dbfs:/Volumes/aiubereats/payments/raw_json/
```

---

## Deploy and Run the Pipeline

### Deploy with Databricks Asset Bundles

```bash
# Validate the bundle configuration
databricks bundle validate

# Deploy to the dev target (default)
databricks bundle deploy

# Deploy to a specific target
databricks bundle deploy --target dev
```

### Run the pipeline

```bash
# Trigger a one-off run of the full pipeline job
databricks bundle run medallion_pipeline

# Monitor the run in the Databricks UI or via CLI
databricks jobs list
```

The job executes three tasks in sequence:

```
bronze  →  silver  →  gold
```

Each task passes `catalog`, `schema`, and `volume` as widget parameters from `databricks.yml`.

### Run notebooks interactively

Open any notebook in the Databricks workspace and run cells top-to-bottom. Widget defaults point to the production tables — override them for sandbox runs:

```python
dbutils.widgets.set("bronze_table", "aiubereats.payments.bronze_payment_events_dev")
```

---

## Key Technical Decisions

### Idempotent MERGE on business keys

All three layers use `MERGE` rather than `APPEND` or `OVERWRITE`. Re-running the pipeline with the same source data produces no duplicates and no data loss. Bronze merges on `event_id`; Silver upserts newer Bronze records; Gold refreshes all columns for changed `payment_id`s.

### Explicit schema in Bronze

Spark's JSON schema inference classifies `1.7596876023E12` as `DoubleType`. An explicit `StructType` with `LongType` for `event.timestamp` forces correct numeric coercion and documents intent. The Bronze sanity-check cell asserts the dtype is `LongType` on every run.

### Liquid Clustering instead of static partitioning

Silver and Gold use `CLUSTER BY` (Delta Liquid Clustering) rather than `PARTITIONED BY`. Liquid Clustering is the recommended approach on Databricks Serverless — it avoids small-file proliferation, adapts to query patterns at compaction time, and does not require choosing partition columns upfront.

### Quarantine pattern

Rows that fail Silver quality gates (null business keys, invalid `event_name` enum values, null `event.timestamp`) are written to `quarantine_payment_events` with a `_quarantine_reason` label rather than silently dropped or failing the job. The main pipeline continues with valid rows.

### Spark Connect compatibility

All operations use the DataFrame/SQL API exclusively. No `sparkContext`, no `mapPartitions`, no accumulators. `_metadata.file_path` is used instead of `input_file_name()` (which requires `TaskContext` unavailable in Serverless). `timestamp_seconds()` replaces UDF-based epoch conversions.

### Change Data Feed on Silver

Silver enables `delta.enableChangeDataFeed = true`. This allows future incremental Gold refreshes to read only changed Silver rows via `table_changes()`, avoiding full Silver scans on each pipeline run.

---

## Next Steps

- **dbt integration**: Model the Gold layer as a dbt model on top of Silver for analyst-friendly SQL transformations and built-in `dbt test` data quality.
- **Schema evolution**: Evaluate `CONSTRAINT` columns and `CHECK` constraints in the Silver DDL as the source schema stabilises.
- **Databricks SQL Alerts**: Add a quarantine-rate dashboard alert in Databricks SQL for continuous monitoring beyond the pipeline's built-in 5% threshold check.
- **staging/prod promotion**: Configure GitHub Environment approvals so `databricks bundle deploy --target prod` requires manual sign-off from a reviewer.

---

## License

Private repository — all rights reserved.
