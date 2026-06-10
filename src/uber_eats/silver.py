from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

VALID_EVENT_NAMES = {"created", "authorized", "captured"}
DT_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"
QUARANTINE_RATE_THRESHOLD = 0.05


def apply_quality_gate(df: DataFrame) -> DataFrame:
    valid_names_list = list(VALID_EVENT_NAMES)
    return df.withColumn(
        "_quarantine_reason",
        F.when(F.col("event_id").isNull(),          F.lit("event_id is null"))
         .when(F.col("payment_id").isNull(),         F.lit("payment_id is null"))
         .when(F.col("event").isNull(),              F.lit("event struct is null"))
         .when(
             ~F.col("event.event_name").isin(valid_names_list),
             F.concat(
                 F.lit("invalid event_name: "),
                 F.coalesce(F.col("event.event_name"), F.lit("<null>"))
             )
         )
         .when(F.col("event.timestamp").isNull(),    F.lit("event.timestamp is null"))
         .otherwise(F.lit(None).cast(StringType()))
    )


def check_quarantine_rate(valid_count: int, quarantine_count: int) -> None:
    total = valid_count + quarantine_count
    if total == 0:
        return
    rate = quarantine_count / total
    if rate > QUARANTINE_RATE_THRESHOLD:
        raise ValueError(
            f"Silver quality gate failure: quarantine rate {rate:.1%} exceeds "
            f"{QUARANTINE_RATE_THRESHOLD:.0%} threshold "
            f"({quarantine_count:,} of {total:,} records quarantined). "
            "Investigate upstream data quality before proceeding."
        )


def apply_transformations(valid_df: DataFrame) -> DataFrame:
    return (
        valid_df
        .withColumn("event_name",   F.col("event.event_name"))
        .withColumn(
            "event_timestamp",
            F.timestamp_seconds((F.col("event.timestamp") / 1000).cast("double"))
        )
        .withColumn(
            "dt_current_timestamp",
            F.to_timestamp(F.col("dt_current_timestamp"), DT_FORMAT)
        )
        .withColumn("event_date",           F.to_date(F.col("event_timestamp")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("event")
        .select(
            "event_id",
            "payment_id",
            "event_name",
            "event_timestamp",
            "dt_current_timestamp",
            "event_date",
            "_silver_processed_at",
        )
    )


def deduplicate(df: DataFrame, business_key: str = "event_id") -> DataFrame:
    if "_ingested_at" not in df.columns:
        df = df.withColumn("_ingested_at", F.current_timestamp())
    window = Window.partitionBy(business_key).orderBy(F.col("_ingested_at").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def silver_merge_dedup(
    existing_df: DataFrame,
    incoming_df: DataFrame,
    business_key: str = "event_id",
) -> DataFrame:
    if existing_df.rdd.isEmpty():
        return incoming_df

    matched = (
        incoming_df.alias("s")
        .join(existing_df.alias("t"), business_key)
        .where(F.col("s._silver_processed_at") > F.col("t._silver_processed_at"))
        .select("s.*")
    )
    not_matched = incoming_df.join(
        existing_df.select(business_key), on=business_key, how="left_anti"
    )
    untouched_existing = existing_df.join(
        matched.select(business_key), on=business_key, how="left_anti"
    )
    return untouched_existing.unionByName(matched).unionByName(not_matched)
