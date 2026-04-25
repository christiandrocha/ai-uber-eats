# Warehouse Sizing & Cost Optimization

> **Purpose**: Right-size virtual warehouses by workload type, configure auto-suspend, and control credit consumption
> **MCP Validated**: 2026-04-20

## When to Use

- Setting up a new Snowflake environment
- Reducing unexpected credit consumption
- Handling concurrency spikes without scaling up
- Separating workloads for cost attribution

## Implementation

```sql
-- ─────────────────────────────────────────
-- INGESTION warehouse: small, fast resume
-- ─────────────────────────────────────────
CREATE OR REPLACE WAREHOUSE load_wh WITH
  WAREHOUSE_SIZE   = 'SMALL'
  AUTO_SUSPEND     = 60          -- suspend after 60s of inactivity
  AUTO_RESUME      = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3          -- scale out for concurrent Snowpipe loads
  SCALING_POLICY   = 'ECONOMY'
  COMMENT          = 'Snowpipe and COPY INTO workloads';

-- ─────────────────────────────────────────
-- TRANSFORMATION warehouse: medium, batch ETL
-- ─────────────────────────────────────────
CREATE OR REPLACE WAREHOUSE transform_wh WITH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  COMMENT        = 'Dynamic tables and dbt transformations';

-- ─────────────────────────────────────────
-- ANALYTICS warehouse: large, multi-cluster BI
-- ─────────────────────────────────────────
CREATE OR REPLACE WAREHOUSE analytics_wh WITH
  WAREHOUSE_SIZE    = 'LARGE'
  AUTO_SUSPEND      = 300        -- 5 min — BI users expect fast resume
  AUTO_RESUME       = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 6
  SCALING_POLICY    = 'STANDARD' -- add clusters faster for concurrency
  COMMENT           = 'BI tools and ad-hoc analyst queries';

-- ─────────────────────────────────────────
-- AI warehouse: medium, for Cortex functions
-- ─────────────────────────────────────────
CREATE OR REPLACE WAREHOUSE ai_wh WITH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND   = 120
  AUTO_RESUME    = TRUE
  COMMENT        = 'Cortex AI functions in dynamic tables';

-- ─────────────────────────────────────────
-- DEV warehouse: xsmall, suspend aggressively
-- ─────────────────────────────────────────
CREATE OR REPLACE WAREHOUSE dev_wh WITH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  COMMENT        = 'Developer queries — suspend fast to save credits';
```

## Configuration

| Setting | Recommendation | Description |
|---------|---------------|-------------|
| `AUTO_SUSPEND` | 60s (ETL), 300s (BI) | Seconds of inactivity before suspend |
| `AUTO_RESUME` | TRUE always | Resume automatically on query |
| `SCALING_POLICY` | ECONOMY (batch), STANDARD (BI) | How fast to add clusters |
| `MAX_CLUSTER_COUNT` | 3-6 for BI | Caps concurrency scaling cost |

## Example Usage

```sql
-- View current credit consumption by warehouse (last 7 days)
SELECT
  warehouse_name,
  SUM(credits_used)       AS total_credits,
  SUM(credits_used) * 3.0 AS estimated_cost_usd
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;

-- Identify idle warehouses (running but not querying)
SELECT warehouse_name, AVG(avg_running) AS avg_queries_running
FROM snowflake.account_usage.warehouse_load_history
WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY 1
HAVING avg_queries_running < 0.1
ORDER BY 1;

-- Check query queue depth (sign you need to scale out, not up)
SELECT warehouse_name, AVG(avg_queued_load) AS avg_queue
FROM snowflake.account_usage.warehouse_load_history
WHERE start_time >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;
```

## See Also

- [Architecture Concept](../concepts/architecture.md)
- [Medallion Pipeline Pattern](medallion-pipeline.md)
- [Snowpipe Ingestion Pattern](snowpipe-ingestion.md)
