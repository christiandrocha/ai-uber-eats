# Snowflake Knowledge Base

> **Purpose**: Snowflake cloud data platform — virtual warehouses, Cortex AI, Snowpark, dynamic tables, ingestion, cost optimization
> **MCP Validated**: 2026-04-20

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/architecture.md](concepts/architecture.md) | Virtual warehouses, storage/compute separation, credit model |
| [concepts/cortex-ai.md](concepts/cortex-ai.md) | AI_COMPLETE, AI_EMBED, Cortex Agents, Snowflake Intelligence |
| [concepts/snowpark.md](concepts/snowpark.md) | Python/Java/Scala DataFrames, UDFs, stored procedures, ML |
| [concepts/dynamic-tables.md](concepts/dynamic-tables.md) | Declarative pipelines, TARGET_LAG, tasks/streams CDC |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/medallion-pipeline.md](patterns/medallion-pipeline.md) | Bronze/Silver/Gold with dynamic tables |
| [patterns/cdc-streams-tasks.md](patterns/cdc-streams-tasks.md) | Change data capture with tasks and streams |
| [patterns/snowpipe-ingestion.md](patterns/snowpipe-ingestion.md) | Continuous file ingestion + Snowpipe Streaming |
| [patterns/warehouse-sizing.md](patterns/warehouse-sizing.md) | Right-sizing, auto-suspend, multi-cluster |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Virtual Warehouse** | Compute cluster (XS–6XL) that scales independently from storage |
| **Dynamic Table** | Declarative transformation that refreshes automatically by TARGET_LAG |
| **Cortex AI** | SQL-native LLM functions (AI_COMPLETE, AI_EMBED, AI_CLASSIFY) |
| **Snowpark** | Server-side Python/Java/Scala execution on Snowflake compute |
| **Snowpipe** | Event-driven continuous ingestion from cloud storage |
| **Iceberg Tables** | Open-format tables stored on customer's cloud storage |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/architecture.md, quick-reference.md |
| **Intermediate** | concepts/dynamic-tables.md, patterns/medallion-pipeline.md |
| **Advanced** | concepts/snowpark.md, concepts/cortex-ai.md, patterns/cdc-streams-tasks.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| `data-platform-engineer` | concepts/architecture.md, patterns/warehouse-sizing.md | Platform decisions and cost optimization |
| `dbt-specialist` | patterns/medallion-pipeline.md | dbt on Snowflake patterns |
| `sql-optimizer` | patterns/cdc-streams-tasks.md | Dynamic table and stream SQL |
| `ai-data-engineer` | concepts/cortex-ai.md | Cortex AI and vector embeddings |
