# Snowflake Architecture

> **Purpose**: Virtual warehouses, storage/compute separation, multi-cluster, credit model, and data sharing
> **Confidence**: 0.95
> **MCP Validated**: 2026-04-20

## Overview

Snowflake separates storage, compute, and cloud services into three independent layers. Storage holds data in compressed columnar Parquet files on the cloud provider. Virtual warehouses are ephemeral compute clusters that query storage on demand. The cloud services layer manages metadata, security, and query optimization.

## The Concept

### Storage Layer

- Compressed columnar format (Parquet internally)
- Stored in Snowflake-managed cloud storage (S3/GCS/Azure Blob)
- Micro-partitioned automatically (50–500 MB raw, ~16 MB compressed)
- Time Travel: access historical data up to 90 days
- Fail-safe: 7-day disaster recovery after Time Travel expires

### Compute Layer (Virtual Warehouses)

```sql
-- Create dedicated warehouses by workload type
CREATE WAREHOUSE ingest_wh
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Snowpipe and COPY INTO workloads';

CREATE WAREHOUSE transform_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE WAREHOUSE analytics_wh
  WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 4
  SCALING_POLICY = 'STANDARD';
```

### Multi-Cluster Warehouses

Scale out (more clusters) for concurrency, not scale up (larger size) for single queries:

| Scenario | Solution |
|----------|----------|
| Slow single query | Scale up warehouse size |
| Many concurrent users queuing | Scale out (multi-cluster) |
| Unpredictable spikes | SCALING_POLICY = 'STANDARD' |
| Cost-sensitive concurrency | SCALING_POLICY = 'ECONOMY' |

### Credit Model

| Action | Credit Consumption |
|--------|-------------------|
| Warehouse running | Credits per second (min 60s) |
| Serverless (Snowpipe, Tasks) | Per-compute-unit |
| Storage | $/TB/month (compressed) |
| Data Transfer | Egress charges apply |

### Data Sharing & Marketplace

- Share live data with other Snowflake accounts (no copy)
- Secure Data Sharing: zero-copy, consumer pays compute
- Snowflake Marketplace: publish/subscribe to third-party datasets
- Private Listings: controlled distribution to specific accounts

## Quick Reference

| Feature | Default | Max |
|---------|---------|-----|
| Time Travel | 1 day | 90 days (Enterprise) |
| Fail-safe | 7 days | 7 days |
| Max warehouse size | -- | 6XL (128 credits/hr) |
| Max clusters | -- | 10 per warehouse |
| Result cache | 24 hours | 24 hours |

## Common Mistakes

### Wrong
```sql
-- One giant warehouse for everything
CREATE WAREHOUSE one_warehouse WAREHOUSE_SIZE = 'X4LARGE';
```

### Correct
```sql
-- Dedicated warehouses per workload, right-sized
CREATE WAREHOUSE etl_wh  WAREHOUSE_SIZE = 'MEDIUM' AUTO_SUSPEND = 60;
CREATE WAREHOUSE bi_wh   WAREHOUSE_SIZE = 'LARGE'  MAX_CLUSTER_COUNT = 4;
CREATE WAREHOUSE dev_wh  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60;
```

## Related

- [Dynamic Tables](dynamic-tables.md)
- [Warehouse Sizing Pattern](../patterns/warehouse-sizing.md)
