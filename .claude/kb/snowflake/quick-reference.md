# Snowflake Quick Reference

> Fast lookup tables. For code examples, see linked files.

## Warehouse Sizes

| Size | Credits/hr | Use Case |
|------|-----------|----------|
| XS | 1 | Dev, light queries |
| S | 2 | Small BI workloads |
| M | 4 | Typical analytics |
| L | 8 | Heavy transforms |
| XL | 16 | Large batch jobs |
| 2XL–6XL | 32–128 | Data science, heavy ETL |

## Cortex AI Functions

| Function | Purpose | Input |
|----------|---------|-------|
| `AI_COMPLETE` | Text generation / multimodal | model, prompt |
| `AI_CLASSIFY` | Categorize text or images | categories list |
| `AI_EMBED` | Vector embeddings | model, text |
| `AI_SIMILARITY` | Compare two inputs | model, text_a, text_b |
| `AI_FILTER` | Boolean LLM classification | prompt |
| `AI_TRANSCRIBE` | Audio/video to text | file reference |
| `AI_TRANSLATE` | Language translation | target_language |
| `AI_REDACT` | PII removal | text |

## Ingestion Patterns

| Method | Latency | Trigger | Format |
|--------|---------|---------|--------|
| COPY INTO | Batch | Manual/task | Files |
| Snowpipe | 1-2 min | S3/GCS/Azure notification | Files |
| Snowpipe Streaming | Sub-second | Client SDK | Rows |
| Dynamic Tables | TARGET_LAG | Automatic | SQL |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Multi-hop ELT pipeline | Dynamic Tables |
| CDC from source table | Tasks + Streams |
| Continuous file ingestion | Snowpipe AUTO_INGEST |
| Real-time row inserts | Snowpipe Streaming |
| LLM inference on table data | Cortex AI_COMPLETE |
| Python ML in Snowflake | Snowpark |
| Open-format interoperability | Iceberg Tables |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Set gold TARGET_LAG = '1 minute' | Use '1 hour' for aggregated tables |
| Leave warehouses running idle | Set AUTO_SUSPEND = 60 |
| Run Snowpark without warehouse | Always attach a warehouse |
| Skip `WHEN SYSTEM$STREAM_HAS_DATA` | Add the condition to avoid empty runs |

## Related Documentation

| Topic | Path |
|-------|------|
| Architecture | `concepts/architecture.md` |
| Cortex AI | `concepts/cortex-ai.md` |
| Dynamic Tables | `concepts/dynamic-tables.md` |
| Full Index | `index.md` |
