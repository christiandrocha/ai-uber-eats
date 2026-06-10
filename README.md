# ai-uber-eats

> Medallion data pipeline for Uber Eats payment events — Bronze → Silver → Gold on Databricks Serverless with Unity Catalog.

[![CI](https://github.com/christiandrocha/ai-uber-eats/actions/workflows/ci.yml/badge.svg)](https://github.com/christiandrocha/ai-uber-eats/actions/workflows/ci.yml)
[![CD](https://github.com/christiandrocha/ai-uber-eats/actions/workflows/cd.yml/badge.svg)](https://github.com/christiandrocha/ai-uber-eats/actions/workflows/cd.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue)
![Coverage 100%](https://img.shields.io/badge/coverage-100%25-brightgreen)

**Stack:** Databricks Serverless · Delta Lake · PySpark 4.1 · dbt-databricks · Unity Catalog · Databricks Asset Bundles · GitHub Actions · Python 3.12 · pytest · ruff

---

## Overview

Payment teams need to track the full lifecycle of a transaction — from creation through authorization to capture — and measure how long each step takes. Raw payment events arrive as JSON in a shared Unity Catalog Volume alongside data from other Uber Eats domains (GPS, orders, restaurants), with timestamps encoded in scientific notation and no schema enforcement.

This pipeline ingests **507K+ payment events per run**, isolates them from mixed-domain source data, applies a 5-rule quality gate with quarantine, and delivers one clean row per payment with lifecycle timestamps and timing metrics to an analytics-ready Gold table — all idempotent and re-runnable without producing duplicates.

The pipeline runs as a Databricks Job deployed via **Databricks Asset Bundles (DABs)** across dev, staging, and prod environments. All transformation logic lives in the importable `uber_eats` Python package, keeping notebooks thin and every business rule unit-testable locally without a Databricks connection.

---

## Architecture

```
/Volumes/aiubereats/payments/raw_json/
           │  JSON files (mixed event types — payment events filtered at Bronze)
           ▼
┌──────────────────────────────────────┐
│   Bronze Layer  [PySpark]            │  Filter to payment events, explicit schema,
│   bronze_payment_events              │  idempotent MERGE on event_id,
│                                      │  partition by _ingested_date
└──────────────────┬───────────────────┘
                   │  incremental read (_ingested_at watermark)
                   ▼
┌──────────────────────────────────────┐
│   Silver Layer  [PySpark]            │  Quality gate (5 rules), quarantine pattern,
│   silver_payment_events              │  dedup, type casting, MERGE on event_id,
│   quarantine_payment_events          │  Liquid Clustering, Change Data Feed
└──────────────────┬───────────────────┘
                   │  dbt source ref
                   ▼
┌──────────────────────────────────────┐
│   Gold Layer  [dbt]                  │  One row per payment_id — lifecycle summary,
│   gold_payment_summary               │  timing metrics, incremental MERGE,
│                                      │  schema tests + custom tests
└──────────────────────────────────────┘
```

---

## Repository Structure

```
ai-uber-eats/
├── .github/
│   └── workflows/
│       ├── ci.yml              # Lint + test + bundle validate (on every push/PR)
│       └── cd.yml              # Deploy + run Bronze/Silver + dbt run/test (on master)
├── notebooks/
│   ├── 01_bronze.ipynb         # Raw ingestion — filter, schema, MERGE
│   ├── 02_silver.ipynb         # Cleanse, deduplicate, quarantine, incremental read
│   └── 03_gold.ipynb           # Reference only — Gold is now managed by dbt
├── dbt/
│   ├── dbt_project.yml         # dbt project config — name, paths, materialization
│   ├── profiles.yml            # Databricks connection (reads from env vars)
│   ├── models/
│   │   ├── sources.yml         # silver_payment_events declared as dbt source
│   │   └── gold/
│   │       ├── gold_payment_summary.sql   # Gold aggregation — incremental MERGE
│   │       └── gold_payment_summary.yml   # Schema tests: unique, not_null, accepted_values
│   └── tests/
│       └── assert_no_negative_times.sql   # Custom test — time metrics ≥ 0
├── src/
│   └── uber_eats/
│       ├── __init__.py         # Public API re-exports
│       ├── bronze.py           # add_metadata_columns, merge_dedup, validate_structure
│       ├── silver.py           # apply_quality_gate, apply_transformations, deduplicate,
│       │                       #   silver_merge_dedup, check_quarantine_rate + constants
│       └── gold.py             # build_gold, gold_merge (used by unit tests)
├── tests/
│   ├── conftest.py             # Shared SparkSession fixture and test schemas
│   ├── test_bronze.py          # 22 tests — schema, metadata, dedup, validation
│   ├── test_silver.py          # 34 tests — quality gate, transforms, dedup, idempotency
│   └── test_gold.py            # 19 tests — aggregation, status, timing, idempotency
├── databricks.yml              # DABs bundle — Bronze + Silver job, dev/staging/prod targets
├── pyproject.toml              # pytest + coverage + ruff config
├── requirements-dev.txt        # Pinned dev dependencies (includes dbt-databricks)
└── .pre-commit-config.yaml     # ruff check + ruff-format on commit
```

---

## Source Data Schema

The source volume contains JSON files with events from multiple Uber Eats domains (orders, GPS, restaurants, drivers, etc.). Bronze filters to **payment event records** only — those where `event_id`, `payment_id`, and `event` are all present.

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

`event.timestamp` may arrive in scientific notation (`1.7596876023E12`). The explicit Bronze schema forces `LongType` coercion so precision is preserved — see the sanity-check cell in `01_bronze.ipynb`.

---

## Layer Reference

### Bronze — Raw Ingestion

**Table**: `aiubereats.payments.bronze_payment_events`

Reads JSON from the Unity Catalog Volume with an explicit `StructType` schema, filters to payment event records, adds ingestion metadata, and MERGEs on `event_id` for idempotent re-runs. A one-time `DELETE WHERE event_id IS NULL` cleanup runs on each execution (no-op once the table is clean).

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
**Quarantine**: `aiubereats.payments.quarantine_payment_events`

Reads Bronze **incrementally** (only records with `_ingested_at` newer than Silver's current watermark). Applies a 5-rule quality gate, writes rejected rows to the quarantine table with a `_quarantine_reason` label, and raises a `ValueError` if the quarantine rate exceeds **5%** to surface upstream data quality incidents. Valid rows are deduplicated on `event_id`, type-cast, struct-flattened, and MERGEd into Silver.

**Quality gate rules (first failing rule wins):**

| Rule | Quarantine reason |
|------|------------------|
| `event_id` is null | `event_id is null` |
| `payment_id` is null | `payment_id is null` |
| `event` struct is null | `event struct is null` |
| `event.event_name` not in `{created, authorized, captured}` | `invalid event_name: <value>` |
| `event.timestamp` is null | `event.timestamp is null` |

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING NOT NULL | Business key |
| `payment_id` | STRING NOT NULL | Payment foreign key |
| `event_name` | STRING NOT NULL | One of `created`, `authorized`, `captured` |
| `event_timestamp` | TIMESTAMP NOT NULL | Epoch ms cast to timestamp |
| `dt_current_timestamp` | TIMESTAMP | Source string cast to timestamp |
| `event_date` | DATE NOT NULL | Derived from `event_timestamp` |
| `_ingested_at` | TIMESTAMP | Carried from Bronze |
| `_source_file` | STRING | Carried from Bronze |
| `_source_system` | STRING | Carried from Bronze |
| `_silver_processed_at` | TIMESTAMP | When this row was last processed by Silver |

### Gold — Payment Lifecycle Summary

**Table**: `aiubereats.payments.gold_payment_summary`

One row per `payment_id`. Pivots events via conditional aggregation to derive lifecycle timestamps and three timing metrics. MERGEs unconditionally on `payment_id` so status updates propagate on each run.

| Column | Type | Description |
|--------|------|-------------|
| `payment_id` | STRING NOT NULL | Business key |
| `created_at` | TIMESTAMP | Timestamp of the `created` event |
| `authorized_at` | TIMESTAMP | Timestamp of the `authorized` event |
| `captured_at` | TIMESTAMP | Timestamp of the `captured` event |
| `payment_status` | STRING NOT NULL | `captured` / `authorized` / `created` |
| `auth_time_seconds` | DOUBLE | `authorized_at − created_at` (null if not authorized) |
| `capture_time_seconds` | DOUBLE | `captured_at − authorized_at` (null if not captured) |
| `total_processing_time_seconds` | DOUBLE | `captured_at − created_at` (null if not captured) |
| `event_count` | LONG NOT NULL | Distinct events observed for this payment |
| `_computed_at` | TIMESTAMP NOT NULL | When this Gold row was last recomputed |

---

## Pipeline Output

After a full run, the Gold table contains one row per `payment_id` with the complete lifecycle:

```
+------------+---------------------+---------------------+---------------------+----------------+------------------+---------------------+------------------------------+-------------+
| payment_id | created_at          | authorized_at       | captured_at         | payment_status | auth_time_seconds| capture_time_seconds| total_processing_time_seconds| event_count |
+------------+---------------------+---------------------+---------------------+----------------+------------------+---------------------+------------------------------+-------------+
| pay-A      | 2025-10-05 18:00:00 | 2025-10-05 18:00:30 | 2025-10-05 18:01:30 | captured       | 30.0             | 60.0                | 90.0                         | 3           |
| pay-B      | 2025-10-05 19:00:00 | 2025-10-05 19:00:45 | null                | authorized     | 45.0             | null                | null                         | 2           |
| pay-C      | 2025-10-05 20:00:00 | null                | null                | created        | null             | null                | null                         | 1           |
+------------+---------------------+---------------------+---------------------+----------------+------------------+---------------------+------------------------------+-------------+
```

**What this enables:**

| Question | Answer from Gold |
|----------|-----------------|
| What is the capture rate? | `COUNT(*) WHERE payment_status = 'captured' / COUNT(*)` |
| What is the average end-to-end processing time? | `AVG(total_processing_time_seconds)` WHERE captured |
| Which payments are stuck in authorization? | Filter `payment_status = 'authorized'` + `authorized_at < now() - interval 1 hour` |
| How many events did each payment produce? | `event_count` column |
| Did any payments skip authorization and go straight to capture? | `captured_at IS NOT NULL AND authorized_at IS NULL` |

---

## Python Package

Transformation logic is extracted into the `uber_eats` package under `src/`. Notebooks import from it; tests import the same functions — no logic duplication.

```python
from uber_eats.bronze import add_metadata_columns, merge_dedup, validate_structure
from uber_eats.silver import (
    apply_quality_gate,
    apply_transformations,
    check_quarantine_rate,
    deduplicate,
    silver_merge_dedup,
    VALID_EVENT_NAMES,
    DT_FORMAT,
)
from uber_eats.gold import build_gold, gold_merge
```

---

## CI/CD Pipeline

```
push / pull_request
        │
        ▼
┌───────────────────────────────────┐
│  CI (runs on every push/PR)       │
│  ├─ ruff check .                  │  Lint
│  ├─ pytest (75 tests, cov ≥ 80%)  │  Unit tests + coverage
│  └─ databricks bundle validate    │  Bundle config check
└───────────────────┬───────────────┘
                    │ CI success on master
                    ▼
┌───────────────────────────────────┐
│  CD (master only, CI must pass)   │
│  ├─ databricks bundle deploy      │  Deploy notebooks + job
│  └─ databricks bundle run         │  Execute Bronze→Silver→Gold
└───────────────────────────────────┘
```

The CD workflow triggers via `workflow_run` and only fires when CI passes — no separate branch protection rules required.

---

## dbt — Gold Layer

The Gold layer is managed by [dbt-databricks](https://github.com/databricks/dbt-databricks). It reads from `silver_payment_events` and produces `gold_payment_summary` via an incremental MERGE on `payment_id`.

### Models

| Model | Materialization | Description |
|-------|----------------|-------------|
| `gold_payment_summary` | incremental (merge) | One row per payment — lifecycle timestamps, status, timing metrics |

### Tests

| Test | Type | What it checks |
|------|------|---------------|
| `unique(payment_id)` | schema | No duplicate payment_ids in Gold |
| `not_null(payment_id)` | schema | Business key is always present |
| `not_null(payment_status)` | schema | Every payment has a status |
| `accepted_values(payment_status)` | schema | Only `created`, `authorized`, `captured` |
| `not_null(event_count)` | schema | Event count is always populated |
| `assert_no_negative_times` | custom | Time metrics are never negative |

### Running dbt locally

```bash
# Set required environment variables
export DATABRICKS_HOST=https://dbc-f3701868-1581.cloud.databricks.com
export DATABRICKS_TOKEN=<your-token>
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>

# Run Gold model
dbt run --profiles-dir dbt --project-dir dbt

# Run all tests
dbt test --profiles-dir dbt --project-dir dbt

# Full refresh (rebuilds Gold from scratch)
dbt run --profiles-dir dbt --project-dir dbt --full-refresh

# Generate and serve documentation
dbt docs generate --profiles-dir dbt --project-dir dbt
dbt docs serve --project-dir dbt
```

### Required GitHub Secret

Add `DATABRICKS_HTTP_PATH` to your repository secrets (Settings → Secrets → Actions):

```
Name:  DATABRICKS_HTTP_PATH
Value: /sql/1.0/warehouses/<your-warehouse-id>
```

Find your warehouse HTTP path: Databricks workspace → SQL Warehouses → select warehouse → **Connection Details** tab → HTTP Path.

---

## Environment Setup

### Prerequisites

| Requirement | Version / Notes |
|-------------|----------------|
| Databricks workspace | Serverless enabled |
| Databricks CLI | `v0.200+` |
| Python | 3.12+ |
| Java | 11+ (for local PySpark tests) |
| Unity Catalog | Catalog `aiubereats`, schema `payments`, volume `raw_json` |

### 1. Clone and install dev dependencies

```bash
git clone https://github.com/christiandrocha/ai-uber-eats.git
cd ai-uber-eats
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
```

### 2. Configure the Databricks CLI

```bash
databricks configure --host https://<your-workspace>.cloud.databricks.com
# Enter your personal access token when prompted
```

### 3. Upload source JSON files

```bash
databricks fs cp --recursive ./data/ \
  dbfs:/Volumes/aiubereats/payments/raw_json/
```

---

## Local Development

### Run tests

```bash
# Set JAVA_HOME if java is not on PATH
export JAVA_HOME=/path/to/jdk17

pytest                     # All 75 tests + coverage report
pytest tests/test_silver.py  # Single file
```

### Lint

```bash
ruff check .               # Check
ruff check --fix .         # Auto-fix
```

### Pre-commit hooks (one-time setup)

```bash
pip install pre-commit
pre-commit install
```

---

## Deploy and Run

### Validate, deploy and run

```bash
# Validate bundle config
databricks bundle validate

# Deploy to dev (default)
databricks bundle deploy

# Run the full pipeline
databricks bundle run medallion_pipeline

# Deploy to specific target
databricks bundle deploy --target staging
databricks bundle deploy --target prod
```

### Available targets

| Target | Catalog | Mode |
|--------|---------|------|
| `dev` (default) | `aiubereats` | development |
| `staging` | `aiubereats_staging` | production |
| `prod` | `aiubereats` | production |

---

## Key Design Decisions

### Multi-event type filtering in Bronze

The source volume contains records from multiple Uber Eats domains (GPS, orders, restaurants, products). Bronze filters to payment event records — those with `event_id`, `payment_id`, and `event` all non-null — before the MERGE. Without this filter, non-payment records with `NULL` event_ids accumulate in Bronze on every run (since `NULL = NULL` is `FALSE` in the MERGE condition).

### Explicit schema in Bronze

Spark's JSON schema inference classifies `1.7596876023E12` as `DoubleType`. An explicit `StructType` with `LongType` for `event.timestamp` forces correct numeric coercion and prevents precision loss downstream.

### Incremental reads in Silver

Silver reads only Bronze records with `_ingested_at > max(silver._ingested_at)`. On the first run (or when Silver is empty) it performs a full Bronze read. This prevents reprocessing the full dataset on every pipeline execution.

### Quarantine rate guard

If more than 5% of Bronze records fail the Silver quality gate, the pipeline raises a `ValueError` and stops before writing. This surfaces upstream data quality incidents immediately rather than silently degrading the Silver and Gold tables.

### Idempotent MERGE on business keys

All three layers use `MERGE` on their business keys (`event_id` for Bronze and Silver, `payment_id` for Gold). Re-running the pipeline with the same source data produces no duplicates and no data loss.

### Liquid Clustering over static partitioning

Silver and Gold use `CLUSTER BY` (Delta Liquid Clustering) rather than `PARTITIONED BY`. This avoids small-file proliferation, adapts to query patterns at compaction time, and is the recommended approach on Databricks Serverless.

### Change Data Feed on Silver

Silver enables `delta.enableChangeDataFeed = true`, allowing future incremental Gold refreshes via `table_changes()` to read only changed rows.

### Spark Connect compatibility

All operations use the DataFrame/SQL API exclusively — no `sparkContext`, no RDDs, no `mapPartitions`. `_metadata.file_path` replaces `input_file_name()` (which requires `TaskContext` unavailable in Serverless).

---

## Next Steps

- **dbt marts**: Add analytical models on top of `gold_payment_summary` — e.g. `payments_captured_under_60s`, `daily_capture_rate` — each as a simple `.sql` file in `dbt/models/`.
- **Schema evolution**: Evaluate `CONSTRAINT` columns and `CHECK` constraints in Silver DDL as the source schema stabilises.
- **Databricks SQL Alerts**: Add a quarantine-rate dashboard alert for continuous monitoring beyond the pipeline's built-in 5% threshold check.
- **staging/prod promotion**: Configure GitHub Environment approvals so `databricks bundle deploy --target prod` requires manual sign-off from a reviewer.
- **dbt docs site**: Publish the dbt documentation site (`dbt docs generate`) to GitHub Pages on each CD run for automatic lineage and column documentation.

---

## License

Private repository — all rights reserved.
