from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_gold(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df
        .groupBy("payment_id")
        .agg(
            F.max(
                F.when(F.col("event_name") == "created",    F.col("event_timestamp"))
            ).alias("created_at"),
            F.max(
                F.when(F.col("event_name") == "authorized", F.col("event_timestamp"))
            ).alias("authorized_at"),
            F.max(
                F.when(F.col("event_name") == "captured",   F.col("event_timestamp"))
            ).alias("captured_at"),
            F.count("event_id").alias("event_count"),
        )
        .withColumn(
            "payment_status",
            F.when(F.col("captured_at").isNotNull(),   F.lit("captured"))
             .when(F.col("authorized_at").isNotNull(), F.lit("authorized"))
             .otherwise(                               F.lit("created"))
        )
        .withColumn(
            "auth_time_seconds",
            F.when(
                F.col("authorized_at").isNotNull() & F.col("created_at").isNotNull(),
                F.unix_timestamp(F.col("authorized_at")) - F.unix_timestamp(F.col("created_at"))
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn(
            "capture_time_seconds",
            F.when(
                F.col("captured_at").isNotNull() & F.col("authorized_at").isNotNull(),
                F.unix_timestamp(F.col("captured_at")) - F.unix_timestamp(F.col("authorized_at"))
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn(
            "total_processing_time_seconds",
            F.when(
                F.col("captured_at").isNotNull() & F.col("created_at").isNotNull(),
                F.unix_timestamp(F.col("captured_at")) - F.unix_timestamp(F.col("created_at"))
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn("_computed_at", F.current_timestamp())
        .select(
            "payment_id",
            "created_at",
            "authorized_at",
            "captured_at",
            "payment_status",
            "auth_time_seconds",
            "capture_time_seconds",
            "total_processing_time_seconds",
            "event_count",
            "_computed_at",
        )
    )


def gold_merge(
    existing_df: DataFrame,
    incoming_df: DataFrame,
    business_key: str = "payment_id",
) -> DataFrame:
    if existing_df.rdd.isEmpty():
        return incoming_df

    updated = incoming_df.join(
        existing_df.select(business_key), on=business_key, how="inner"
    )
    new_rows = incoming_df.join(
        existing_df.select(business_key), on=business_key, how="left_anti"
    )
    untouched = existing_df.join(
        updated.select(business_key), on=business_key, how="left_anti"
    )
    return untouched.unionByName(updated).unionByName(new_rows)
