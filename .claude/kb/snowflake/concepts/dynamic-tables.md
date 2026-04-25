# Dynamic Tables & CDC

> **Purpose**: Declarative pipeline layer with dynamic tables, and change data capture with tasks and streams
> **Confidence**: 0.95
> **MCP Validated**: 2026-04-20

## Overview

Dynamic tables replace imperative task/stream pipelines with a declarative SQL definition. Snowflake automatically determines refresh frequency based on `TARGET_LAG` and builds the dependency graph across chained tables. Tasks and streams remain the right choice for event-driven CDC and complex procedural logic.

## Dynamic Tables

### Core Concept

```sql
-- Define transformation declaratively — Snowflake manages refresh
CREATE OR REPLACE DYNAMIC TABLE silver.cleaned_orders
  TARGET_LAG = '5 minutes'         -- max acceptable staleness
  WAREHOUSE = transform_wh
AS
SELECT
  order_id,
  customer_id,
  amount::FLOAT                                           AS amount_usd,
  UPPER(TRIM(status))                                    AS status,
  CONVERT_TIMEZONE('UTC', order_ts)                      AS order_ts_utc
FROM bronze.raw_orders
WHERE order_id IS NOT NULL;
```

### Chaining Dynamic Tables

```sql
-- Snowflake resolves the dependency graph automatically
CREATE OR REPLACE DYNAMIC TABLE gold.daily_revenue
  TARGET_LAG = '1 hour'
  WAREHOUSE = analytics_wh
AS
SELECT
  DATE_TRUNC('day', order_ts_utc) AS order_date,
  SUM(amount_usd)                 AS total_revenue,
  COUNT(DISTINCT customer_id)     AS unique_customers
FROM silver.cleaned_orders
GROUP BY 1;
```

### TARGET_LAG Guidelines

| Layer | Recommended LAG | Reason |
|-------|----------------|--------|
| Bronze | '1 minute' | Raw ingestion, fast refresh |
| Silver | '5 minutes' | Cleaning and deduplication |
| Gold | '1 hour' | Aggregations rarely need sub-hour |
| Reports | '1 day' | Historical summaries |

## Tasks + Streams (CDC)

### When to Use Over Dynamic Tables

- CDC with DELETE and UPDATE handling (MERGE)
- Complex procedural logic inside the transform
- Custom scheduling (not just lag-based)
- Multiple target tables from one stream

### Implementation

```sql
-- Stream captures INSERT / UPDATE / DELETE on source
CREATE OR REPLACE STREAM raw.orders_stream ON TABLE raw.orders
  APPEND_ONLY = FALSE;

-- Task runs only when stream has data
CREATE OR REPLACE TASK process_orders_cdc
  WAREHOUSE = task_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('raw.orders_stream')
AS
MERGE INTO curated.orders t
USING raw.orders_stream s ON t.order_id = s.order_id
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
  UPDATE SET
    t.amount = s.amount,
    t.status = s.status,
    t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
  INSERT (order_id, customer_id, amount, status, created_at)
  VALUES (s.order_id, s.customer_id, s.amount, s.status, CURRENT_TIMESTAMP());

ALTER TASK process_orders_cdc RESUME;
```

## Common Mistakes

### Wrong
```sql
-- Dynamic table on gold with 1-minute lag wastes credits
CREATE DYNAMIC TABLE gold.monthly_revenue TARGET_LAG = '1 minute' ...
```

### Correct
```sql
-- Match lag to business need, not technical capability
CREATE DYNAMIC TABLE gold.monthly_revenue TARGET_LAG = '1 hour' ...
```

## Related

- [Architecture](architecture.md)
- [Medallion Pipeline Pattern](../patterns/medallion-pipeline.md)
- [CDC Streams Tasks Pattern](../patterns/cdc-streams-tasks.md)
