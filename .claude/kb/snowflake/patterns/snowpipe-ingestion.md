# Snowpipe Ingestion

> **Purpose**: Continuous file ingestion from cloud storage via Snowpipe (event-driven) and Snowpipe Streaming (sub-second SDK)
> **MCP Validated**: 2026-04-20

## When to Use

- **Snowpipe AUTO_INGEST**: Files land in S3/GCS/Azure and need loading within 1-2 minutes
- **Snowpipe Streaming**: Application pushes rows directly with sub-second latency
- Replacing scheduled COPY INTO jobs with event-driven ingestion

## Implementation

### Snowpipe with AUTO_INGEST (S3)

```sql
-- STEP 1: Create storage integration (run once by ACCOUNTADMIN)
CREATE OR REPLACE STORAGE INTEGRATION s3_landing_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789:role/snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/landing/');

-- STEP 2: Create external stage
CREATE OR REPLACE STAGE landing_stage
  URL = 's3://my-bucket/landing/'
  STORAGE_INTEGRATION = s3_landing_int
  FILE_FORMAT = (TYPE = 'PARQUET');

-- STEP 3: Create target table
CREATE OR REPLACE TABLE raw.orders_landing (
  order_id     STRING,
  customer_id  STRING,
  amount       FLOAT,
  status       STRING,
  order_ts     TIMESTAMP_NTZ,
  _loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- STEP 4: Create pipe with auto_ingest
CREATE OR REPLACE PIPE raw.orders_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = 's3_notification_int'
AS
COPY INTO raw.orders_landing
FROM @landing_stage/orders/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- STEP 5: Get SQS ARN to configure S3 event notification
SHOW PIPES LIKE 'orders_pipe';
-- Copy notification_channel value → configure in S3 bucket notifications
```

### Snowpipe Streaming (Python SDK)

```python
from snowflake.ingest import SnowflakeStreamingIngestClient
import json, time

client = SnowflakeStreamingIngestClient(
    name="orders_client",
    account="<account>",
    user="<user>",
    private_key="<private_key>",
)

channel = client.open_channel(
    name="orders_channel",
    database="analytics",
    schema="raw",
    table="orders_landing",
    on_error="CONTINUE",
)

# Insert rows — sub-second latency
rows = [
    {"order_id": "O-001", "customer_id": "C-123", "amount": 99.99, "status": "COMPLETED"},
    {"order_id": "O-002", "customer_id": "C-456", "amount": 149.00, "status": "PENDING"},
]
channel.insert_rows(rows, offset_token=str(time.time()))
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `AUTO_INGEST` | FALSE | TRUE = trigger from cloud storage notifications |
| `ON_ERROR` | ABORT_STATEMENT | CONTINUE skips bad files; ABORT stops on first error |
| `MATCH_BY_COLUMN_NAME` | FALSE | CASE_INSENSITIVE matches Parquet columns to table columns |
| `PURGE` | FALSE | Delete stage files after successful load |

## Example Usage

```sql
-- Check pipe status and backlog
SELECT SYSTEM$PIPE_STATUS('raw.orders_pipe');

-- View ingestion history (last hour)
SELECT *
FROM TABLE(information_schema.copy_history(
  table_name => 'orders_landing',
  start_time => DATEADD('hour', -1, CURRENT_TIMESTAMP())
));

-- Manually refresh pipe (re-scan stage for missed files)
ALTER PIPE raw.orders_pipe REFRESH;

-- Pause / resume pipe
ALTER PIPE raw.orders_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE raw.orders_pipe SET PIPE_EXECUTION_PAUSED = FALSE;
```

## See Also

- [CDC Streams Tasks Pattern](cdc-streams-tasks.md)
- [Medallion Pipeline Pattern](medallion-pipeline.md)
- [Architecture Concept](../concepts/architecture.md)
