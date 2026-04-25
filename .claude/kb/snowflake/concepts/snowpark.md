# Snowpark

> **Purpose**: Server-side Python/Java/Scala execution on Snowflake compute — DataFrames, UDFs, stored procedures, and ML
> **Confidence**: 0.90
> **MCP Validated**: 2026-04-20

## Overview

Snowpark pushes Python, Java, and Scala computation into Snowflake warehouses. Code runs server-side — no data egress, full governance. It replaces the need to extract data for external processing and supports DataFrames, UDFs, UDTFs, stored procedures, and the Snowpark ML library.

## The Concept

### DataFrames (Python)

```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, when

session = Session.builder.configs({
    "account": "<account>",
    "user": "<user>",
    "password": "<password>",
    "warehouse": "transform_wh",
    "database": "analytics",
    "schema": "silver",
}).create()

# Lazy evaluation — pushes SQL to Snowflake
df = session.table("raw.orders")

result = (
    df
    .filter(col("status") == "COMPLETED")
    .with_column("total_usd", col("amount") * lit(1.0))
    .group_by("customer_id")
    .agg({"total_usd": "sum", "order_id": "count"})
    .rename("SUM(TOTAL_USD)", "lifetime_value")
    .rename("COUNT(ORDER_ID)", "order_count")
)

result.write.mode("overwrite").save_as_table("silver.customer_lifetime_value")
```

### Python UDFs

```python
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType

@udf(name="clean_phone", replace=True, return_type=StringType(), input_types=[StringType()])
def clean_phone(phone: str) -> str:
    import re
    return re.sub(r"[^\d]", "", phone or "") if phone else None
```

```sql
-- Call from SQL after registration
SELECT clean_phone(phone_number) FROM customers;
```

### Stored Procedures

```python
from snowflake.snowpark import Session

def run_daily_refresh(session: Session, target_date: str) -> str:
    df = session.table("raw.events").filter(col("event_date") == target_date)
    df.write.mode("append").save_as_table("silver.events")
    return f"Loaded {df.count()} rows for {target_date}"

session.sproc.register(
    func=run_daily_refresh,
    name="daily_events_refresh",
    replace=True,
    is_permanent=True,
    stage_location="@my_stage",
)
```

```sql
CALL daily_events_refresh('2026-04-20');
```

### Snowpark ML

```python
from snowflake.ml.modeling.preprocessing import StandardScaler
from snowflake.ml.modeling.linear_model import LinearRegression

# Feature engineering
scaler = StandardScaler(input_cols=["amount", "order_count"], output_cols=["amount_scaled", "count_scaled"])
df_scaled = scaler.fit(df).transform(df)

# Training
model = LinearRegression(input_cols=["amount_scaled", "count_scaled"], label_cols=["churn"])
model.fit(df_train)
predictions = model.predict(df_test)
```

## Quick Reference

| API | Server-Side | Use Case |
|-----|------------|---------|
| DataFrame | Yes | Data transformation pipelines |
| Python UDF | Yes | Custom scalar functions in SQL |
| Python UDTF | Yes | Table-valued functions |
| Stored Procedure | Yes | Complex multi-step logic |
| Snowpark ML | Yes | Training and inference in Snowflake |

## Common Mistakes

### Wrong
```python
# Calling .collect() early forces all data to client
rows = df.collect()
result = [transform(r) for r in rows]  # data left Snowflake
```

### Correct
```python
# Keep transformations server-side, collect only the final small result
df_transformed = df.filter(...).group_by(...).agg(...)
df_transformed.write.save_as_table("output")  # stays server-side
```

## Related

- [Cortex AI](cortex-ai.md)
- [Medallion Pipeline Pattern](../patterns/medallion-pipeline.md)
