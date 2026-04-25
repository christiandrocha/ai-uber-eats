# Medallion Pipeline with Dynamic Tables

> **Purpose**: Bronze/Silver/Gold architecture using Snowflake dynamic tables for declarative, auto-refreshing pipelines
> **MCP Validated**: 2026-04-20

## When to Use

- Building multi-hop ELT pipelines in Snowflake
- Replacing complex task/stream chains with declarative SQL
- Continuous ingestion from Snowpipe into Medallion layers
- AI-enriched silver or gold layers using Cortex functions

## Implementation

```sql
-- ─────────────────────────────────────────
-- BRONZE: Raw ingestion from landing stage
-- ─────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE bronze.raw_events
  TARGET_LAG = '1 minute'
  WAREHOUSE = load_wh
AS
SELECT
  $1:event_id::STRING        AS event_id,
  $1:event_type::STRING      AS event_type,
  $1:user_id::STRING         AS user_id,
  $1:properties::VARIANT     AS properties,
  $1:timestamp::TIMESTAMP_NTZ AS event_ts,
  METADATA$FILENAME           AS source_file,
  CURRENT_TIMESTAMP()         AS loaded_at
FROM @landing_stage/events/;

-- ─────────────────────────────────────────
-- SILVER: Cleaned, deduplicated, typed
-- ─────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE silver.events
  TARGET_LAG = '5 minutes'
  WAREHOUSE = transform_wh
AS
SELECT
  event_id,
  event_type,
  user_id,
  properties:page::STRING             AS page,
  properties:duration::FLOAT          AS duration_sec,
  CONVERT_TIMEZONE('UTC', event_ts)   AS event_ts_utc,
  loaded_at
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY loaded_at DESC) AS rn
  FROM bronze.raw_events
)
WHERE rn = 1
  AND event_id IS NOT NULL
  AND user_id  IS NOT NULL;

-- ─────────────────────────────────────────
-- SILVER (AI-enriched): Cortex classification
-- ─────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE silver.enriched_events
  TARGET_LAG = '10 minutes'
  WAREHOUSE = ai_wh
AS
SELECT
  event_id,
  event_type,
  user_id,
  event_ts_utc,
  AI_CLASSIFY(
    event_type,
    ['acquisition', 'engagement', 'conversion', 'retention']
  )::VARCHAR AS funnel_stage
FROM silver.events;

-- ─────────────────────────────────────────
-- GOLD: Aggregated daily metrics
-- ─────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE gold.daily_event_metrics
  TARGET_LAG = '1 hour'
  WAREHOUSE = analytics_wh
AS
SELECT
  DATE_TRUNC('day', event_ts_utc)   AS event_date,
  event_type,
  funnel_stage,
  COUNT(*)                           AS event_count,
  COUNT(DISTINCT user_id)            AS unique_users,
  AVG(duration_sec)                  AS avg_duration_sec
FROM silver.enriched_events
GROUP BY 1, 2, 3;
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `TARGET_LAG` | required | Max staleness: '1 minute', '5 minutes', '1 hour', '1 day' |
| `WAREHOUSE` | required | Warehouse to use for refresh compute |
| `REFRESH_MODE` | AUTO | AUTO / FULL / INCREMENTAL |
| `INITIALIZE` | ON_CREATE | When to run first refresh |

## Example Usage

```sql
-- Monitor dynamic table refresh lag
SELECT
  name,
  target_lag,
  scheduling_state,
  last_completed_dependency_refresh_time
FROM information_schema.dynamic_tables
WHERE schema_name = 'SILVER';

-- Manually force refresh
ALTER DYNAMIC TABLE silver.events REFRESH;

-- Suspend a dynamic table (stop refreshing)
ALTER DYNAMIC TABLE silver.events SUSPEND;
```

## See Also

- [Dynamic Tables Concept](../concepts/dynamic-tables.md)
- [CDC Streams Tasks Pattern](cdc-streams-tasks.md)
- [Warehouse Sizing Pattern](warehouse-sizing.md)
