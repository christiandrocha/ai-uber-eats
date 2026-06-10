from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_metadata_columns(
    df: DataFrame,
    source_file: str = "unknown",
    source_system: str = "uber_eats_payments_api",
) -> DataFrame:
    return (
        df
        .withColumn("_ingested_at",   F.current_timestamp())
        .withColumn("_ingested_date", F.to_date(F.current_timestamp()))
        .withColumn("_source_file",   F.lit(source_file))
        .withColumn("_source_system", F.lit(source_system))
    )


def validate_structure(df: DataFrame, source_path: str = "unknown") -> None:
    row_count = df.count()
    if row_count == 0:
        raise ValueError(
            f"Bronze ingestion aborted: no records found at {source_path}. "
            "Verify the Volume path and that JSON files are present."
        )
    expected_cols = {"event_id", "payment_id", "event", "dt_current_timestamp"}
    missing_cols = expected_cols - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"Bronze ingestion aborted: source files are missing required columns: {missing_cols}"
        )


def merge_dedup(existing_df: DataFrame, incoming_df: DataFrame) -> DataFrame:
    new_rows = (
        incoming_df
        .dropDuplicates(["event_id"])
        .join(existing_df.select("event_id"), on="event_id", how="left_anti")
    )
    return existing_df.unionByName(new_rows, allowMissingColumns=True)
