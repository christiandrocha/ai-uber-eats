---
name: medallion-pipeline-tests
description: Test suite setup, known bugs fixed, and how to run local PySpark tests
type: project
---

Unit test suite lives in `tests/` with 67 pytest tests across Bronze/Silver/Gold layers.

**Why:** Tests run locally without a Databricks connection using PySpark local mode (`local[2]`).

**How to apply:** When modifying notebooks or adding layers, run the tests with:
```
JAVA_HOME=/home/christian/.local/jdk17 .venv/bin/python -m pytest
```

Java 17 JRE is installed at `/home/christian/.local/jdk17` (portable, no sudo needed).
Python venv is at `.venv/` with pyspark==4.1.1 and pytest>=9.0.

Bugs fixed during initial generation:
1. `merge_dedup` in test_bronze.py now calls `.dropDuplicates(["event_id"])` on the incoming batch before the anti-join — Delta recommends deduplicating source before MERGE.
2. `test_epoch_ms_converted_to_timestamp` compares via `ts.timestamp() * 1000` (epoch ms) instead of `.hour == 18` — Spark returns local-timezone naive datetimes, not UTC.
3. `test_dedup_is_deterministic` compares only stable business columns (excludes `_silver_processed_at`, `_ingested_at` which re-evaluate `current_timestamp()` on each action).
4. `test_new_event_inserted_on_rerun` extra_row now includes `_ingested_at` (added by `deduplicate()`, so schema has 8 not 7 fields).
