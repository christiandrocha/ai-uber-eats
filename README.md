# ai-uber-eats

> Medallion data pipeline for Uber Eats payment events — Bronze → Silver → Gold on Databricks Serverless with Unity Catalog.

[![CI](https://github.com/christiandrocha/ai-uber-eats/actions/workflows/ci.yml/badge.svg)](https://github.com/christiandrocha/ai-uber-eats/actions/workflows/ci.yml)
[![CD](https://github.com/christiandrocha/ai-uber-eats/actions/workflows/cd.yml/badge.svg)](https://github.com/christiandrocha/ai-uber-eats/actions/workflows/cd.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue)
![Coverage 100%](https://img.shields.io/badge/coverage-100%25-brightgreen)

---

## Overview

This project implements a production-grade **Medallion Architecture** pipeline that ingests JSON files containing Uber Eats payment events from a mixed-schema Unity Catalog Volume, cleanses and enriches the data through layered transformations, and delivers a business-ready payment lifecycle summary to the Gold layer.

The pipeline runs as a Databricks Job with three sequential notebook tasks deployed via **Databricks Asset Bundles (DABs)**. All compute runs on **Databricks Serverless** using the Spark Connect API. Transformation logic lives in the importable `uber_eats` Python package, keeping notebooks thin and all business logic unit-testable locally without a Databricks connection.

---

## Architecture

```
/Volumes/aiubereats/payments/raw_json/
           │  JSON files (mixed event types — payment events filtered at Bronze)
           ▼
┌──────────────────────────────────────┐
│   Bronze Layer                       │  Filter to payment events, explicit schema,
│   bronze_payment_events              │  idempotent MERGE on event_id,
│                                      │  partition by _ingested_date
└──────────────────┬───────────────────┘
                   │  incremental read (_ingested_at watermark)
                   ▼
┌──────────────────────────────────────┐
│   Silver Layer                       │  Quality gate (5 rules), quarantine pattern,
│   silver_payment_events              │  dedup, type casting, MERGE on event_id,
│   quarantine_payment_events          │  Liquid Clustering, Change Data Feed
└──────────────────┬───────────────────┘
                   ▼
┌──────────────────────────────────────┐
│   Gold Layer                         │  One row per payment_id — lifecycle summary,
│   gold_payment_summary               │  timing metrics, MERGE on payment_id,
│                                      │  Liquid Clustering on payment_status
└──────────────────────────────────────┘
```

---

## Repository Structure

```
ai-uber-eats/
├── .github/
│   └── workflows/
│       ├── ci.yml              # Lint + test + bundle validate (on every push/PR)
│       └── cd.yml              # Deploy + run (on master, gated on CI passing)
├── notebooks/
│   ├── 01_bronze.ipynb         # Raw ingestion — filter, schema, MERGE
│   ├── 02_silver.ipynb         # Cleanse, deduplicate, quarantine, incremental read
│   └── 03_gold.ipynb           # Payment lifecycle aggregation
├── src/
│   └── uber_eats/
│       ├── __init__.py         # Public API re-exports
│       ├── bronze.py           # add_metadata_columns, merge_dedup, validate_structure
│       ├── silver.py           # apply_quality_gate, apply_transformations, deduplicate,
│       │                       #   silver_merge_dedup, check_quarantine_rate + constants
│       └── gold.py             # build_gold, gold_merge
├── tests/
│   ├── conftest.py             # Shared SparkSession fixture and test schemas
│   ├── test_bronze.py          # 22 tests — schema, metadata, dedup, validation
│   ├── test_silver.py          # 34 tests — quality gate, transforms, dedup, idempotency
│   └── test_gold.py            # 19 tests — aggregation, status, timing, idempotency
├── databricks.yml              # DABs bundle — job definition, dev/staging/prod targets
├── pyproject.toml              # pytest + coverage + ruff config
├── requirements-dev.txt        # Pinned dev dependencies
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
│  ├─ pytest (75 tests, cov ≥ 80%) │  Unit tests + coverage
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

- **dbt integration**: Model the Gold layer as a dbt model on top of Silver for analyst-friendly SQL and built-in `dbt test` data quality checks.
- **Schema evolution**: Evaluate `CONSTRAINT` columns and `CHECK` constraints in Silver DDL as the source schema stabilises.
- **Databricks SQL Alerts**: Add a quarantine-rate dashboard alert for continuous monitoring beyond the pipeline's built-in 5% threshold check.
- **staging/prod promotion**: Configure GitHub Environment approvals so `databricks bundle deploy --target prod` requires manual sign-off from a reviewer.

---

## License

Private repository — all rights reserved.
