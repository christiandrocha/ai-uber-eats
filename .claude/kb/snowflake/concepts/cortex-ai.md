# Snowflake Cortex AI

> **Purpose**: SQL-native LLM inference — AI_COMPLETE, AI_EMBED, AI_CLASSIFY, Cortex Agents, and Snowflake Intelligence
> **Confidence**: 0.90
> **MCP Validated**: 2026-04-20

## Overview

Cortex AI embeds LLM-powered functions directly into Snowflake SQL. No infrastructure to manage — data never leaves Snowflake's security perimeter. Seven functions reached GA in November 2025, covering generation, classification, embeddings, similarity, transcription, translation, and PII redaction.

## The Concept

### Cortex AI Functions

| Function | Purpose | Status |
|----------|---------|--------|
| `AI_COMPLETE` | Text generation, Q&A, summarization (multimodal: text + images) | GA Nov 2025 |
| `AI_CLASSIFY` | Classify text or images into categories | GA Nov 2025 |
| `AI_EMBED` | Vector embeddings for semantic search and RAG | GA Nov 2025 |
| `AI_SIMILARITY` | Embedding similarity between two inputs | GA Nov 2025 |
| `AI_TRANSCRIBE` | Audio/video to text | GA Nov 2025 |
| `AI_TRANSLATE` | Language translation | GA |
| `AI_FILTER` | Boolean LLM classification for row-level filtering | GA |
| `AI_REDACT` | Automated PII removal | 2025 |

### Supported Models

- **OpenAI**: GPT-5.2 (text + images)
- **Mistral**: mistral-large2, mistral-7b
- **Meta**: llama-3.1-8b, llama-3.1-70b, llama-3.1-405b
- **Snowflake**: snowflake-arctic-embed-l-v2.0 (embeddings)

### Usage Examples

```sql
-- Text classification
SELECT
  ticket_id,
  AI_CLASSIFY(description, ['billing', 'technical', 'account', 'other'])::VARCHAR AS category
FROM support.tickets;

-- Summarization with AI_COMPLETE
SELECT
  doc_id,
  AI_COMPLETE('mistral-large2', CONCAT('Summarize in 2 sentences: ', content)) AS summary
FROM knowledge_base.documents;

-- Vector embeddings for RAG
SELECT
  doc_id,
  AI_EMBED('snowflake-arctic-embed-l-v2.0', content) AS embedding
FROM knowledge_base.documents;

-- PII redaction
SELECT AI_REDACT(customer_note) AS redacted_note
FROM crm.notes;
```

### Cortex Agents (GA Nov 2025)

Orchestrate across structured and unstructured data:

```
1. Planning    — parse request, split into subtasks
2. Tool Use    — Cortex Search (unstructured) + Cortex Analyst (structured SQL)
3. Reflection  — evaluate result, iterate if needed
4. Response    — return final answer
```

### Dynamic Table Integration

Cortex functions work inside dynamic tables for automated AI enrichment:

```sql
CREATE OR REPLACE DYNAMIC TABLE silver.enriched_tickets
  TARGET_LAG = '10 minutes'
  WAREHOUSE = ai_wh
AS
SELECT
  ticket_id,
  description,
  AI_CLASSIFY(description, ['billing', 'technical', 'account']) AS category,
  AI_COMPLETE('mistral-large2', CONCAT('Sentiment (positive/negative/neutral): ', description)) AS sentiment
FROM bronze.raw_tickets;
```

## Common Mistakes

### Wrong
```sql
-- Running AI_COMPLETE on every row in a huge table without filtering
SELECT AI_COMPLETE('mistral-large2', content) FROM docs;  -- 10M rows = huge bill
```

### Correct
```sql
-- Filter to only unprocessed rows, use incremental dynamic table
SELECT AI_COMPLETE('mistral-large2', content)
FROM docs
WHERE processed_at IS NULL
LIMIT 10000;
```

## Related

- [Snowpark](snowpark.md)
- [Medallion Pipeline Pattern](../patterns/medallion-pipeline.md)
