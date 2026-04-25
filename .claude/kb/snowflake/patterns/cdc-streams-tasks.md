# CDC with Streams and Tasks

> **Purpose**: Change data capture using Snowflake streams and scheduled tasks — handles INSERT, UPDATE, DELETE via MERGE
> **MCP Validated**: 2026-04-20

## When to Use

- Propagating row-level changes (INSERT/UPDATE/DELETE) from source to target
- Complex MERGE logic that dynamic tables can't express declaratively
- Multiple target tables from a single change stream
- Custom scheduling requirements beyond lag-based refresh

## Implementation

```sql
-- ─────────────────────────────────────────
-- STEP 1: Stream on source table
-- ─────────────────────────────────────────
CREATE OR REPLACE STREAM raw.orders_stream
  ON TABLE raw.orders
  APPEND_ONLY = FALSE;     -- capture updates and deletes too

-- ─────────────────────────────────────────
-- STEP 2: Task processes stream changes
-- ─────────────────────────────────────────
CREATE OR REPLACE TASK process_orders_cdc
  WAREHOUSE = task_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('raw.orders_stream')
AS
MERGE INTO curated.orders t
USING (
  -- Resolve conflicts: keep latest change per key
  SELECT *
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY METADATA$ROW_ID DESC
      ) AS rn
    FROM raw.orders_stream
  )
  WHERE rn = 1
) s ON t.order_id = s.order_id
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
  UPDATE SET
    t.customer_id  = s.customer_id,
    t.amount       = s.amount,
    t.status       = s.status,
    t.updated_at   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
  INSERT (order_id, customer_id, amount, status, created_at, updated_at)
  VALUES (s.order_id, s.customer_id, s.amount, s.status, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

ALTER TASK process_orders_cdc RESUME;

-- ─────────────────────────────────────────
-- STEP 3: Task tree for dependent transforms
-- ─────────────────────────────────────────
CREATE OR REPLACE TASK refresh_order_metrics
  WAREHOUSE = analytics_wh
  AFTER process_orders_cdc           -- runs after CDC task completes
AS
INSERT OVERWRITE INTO gold.daily_order_metrics
SELECT
  DATE_TRUNC('day', updated_at) AS order_date,
  COUNT(*)                       AS order_count,
  SUM(amount)                    AS total_revenue
FROM curated.orders
GROUP BY 1;

ALTER TASK refresh_order_metrics RESUME;
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `APPEND_ONLY` | FALSE | FALSE = capture all DML; TRUE = inserts only |
| `SCHEDULE` | required | Cron or interval: '5 MINUTE', 'USING CRON 0 * * * * UTC' |
| `WHEN` | optional | Conditional: skip if no stream data |
| `AFTER` | optional | Task dependency for DAG-style chains |

## Example Usage

```sql
-- Check stream contents before task runs
SELECT METADATA$ACTION, METADATA$ISUPDATE, *
FROM raw.orders_stream
LIMIT 100;

-- Monitor task run history
SELECT name, state, error_message, scheduled_time, completed_time
FROM information_schema.task_history
WHERE name = 'PROCESS_ORDERS_CDC'
ORDER BY scheduled_time DESC
LIMIT 20;

-- Pause a task
ALTER TASK process_orders_cdc SUSPEND;

-- Check stream offset (consumed vs pending)
SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('raw.orders_stream');
```

## See Also

- [Dynamic Tables Concept](../concepts/dynamic-tables.md)
- [Medallion Pipeline Pattern](medallion-pipeline.md)
- [Snowpipe Ingestion Pattern](snowpipe-ingestion.md)
