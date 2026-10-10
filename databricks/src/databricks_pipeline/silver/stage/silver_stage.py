import logging
import uuid

from pyspark.sql import (
    DataFrame,
    SparkSession,
    functions as F,  # noqa
)
from pyspark.sql.types import DoubleType, StringType

from databricks_pipeline.silver.config.silver_config import SilverConfig
from databricks_pipeline.silver.schema.silver_contract import (
    EXPECTED_BRONZE_COLUMNS,
    quarantine_schema_check,
    silver_schema_check,
)

logger = logging.getLogger(__name__)


def normalize_quarantined_df(quarantined_df: DataFrame, quarantine_batch_id) -> DataFrame:

    separated_quarantined_df = quarantined_df.select(
        "quarantine",
        F.struct(*[F.col(c) for c in quarantined_df.columns if c != "quarantine"]).alias("quarantined_rows"),
        F.current_timestamp().alias("quarantined_at"),
    )

    valid_quarantined_df = separated_quarantined_df.withColumns(
        {
            "quarantined_batch_id": F.lit(quarantine_batch_id),
        }
    )

    expected_quarantine_schema = quarantine_schema_check(valid_quarantined_df.schema["quarantined_rows"].dataType)

    if set(valid_quarantined_df.columns) == set(expected_quarantine_schema.fieldNames()):
        for col in valid_quarantined_df.columns:
            if valid_quarantined_df.schema[col].dataType != expected_quarantine_schema[col].dataType:
                raise ValueError(
                    f"Mismatch between dataTypes of validated_quarantine_df and expected_quarantine_schema for column: {col}"
                )
    else:
        raise ValueError("Mismatch between expected columns in validated_quarantine_df and expected_quarantine_schema")

    return valid_quarantined_df


def bronze_data_cleaning(spark: SparkSession, config: SilverConfig) -> tuple[DataFrame, tuple[DataFrame, DataFrame]]:
    bronze_df = spark.readStream.table(config.source_table)

    missing = EXPECTED_BRONZE_COLUMNS - set(bronze_df.columns)

    quarantine_batch_id = str(uuid.uuid4())

    if missing:
        raise ValueError(f"missing column(s): {missing}")

    checked_bronze_df = bronze_df.withColumn(
        "quarantine",
        F.filter(
            F.array(
                F.when(F.col("metadata").isNull(), "null_metadata"),
                F.when(F.col("metadata.device_id").isNull(), "null_metadata_device_id"),
                F.when(F.col("metadata.test_date").isNull(), "null_metadata_test_date"),
                F.when(F.col("metadata.test_id").isNull(), "null_metadata_test_id"),
                F.when(F.col("metadata.testing_software").isNull(), "null_metadata_testing_software"),
                F.when(F.col("metadata.underlying_system").isNull(), "null_metadata_underlying_system"),
                F.when(F.col("snapshots").isNull(), "null_snapshots"),
                F.when(F.size("snapshots") == 0, "empty_snapshots"),
                F.when(F.col("rescued_data").isNotNull(), "rescued_data_present"),
                F.when(F.col("corrupt_record").isNotNull(), "corrupt_record_present"),
                F.when(F.col("source_file").isNull(), "null_source_file"),
                F.when(F.col("execution_id").isNull(), "null_execution_id"),
                F.when(F.col("dbx_ingest_date").isNull(), "null_dbx_ingest_date"),
                F.when(F.col("adls_upload_date").isNull(), "null_adls_upload_date"),
            ),
            lambda reason: reason.isNotNull(),
        ),
    )

    quarantined_bronze_df = checked_bronze_df.filter(F.size("quarantine") > 0)

    validated_bronze_df = checked_bronze_df.filter(F.size("quarantine") == 0)

    sensors_df = validated_bronze_df.withColumn("snapshot", F.explode("snapshots"))

    flattened_sensors_df = sensors_df.select(
        "metadata",
        F.col("snapshot.hardware_name"),
        F.col("snapshot.sensor_name"),
        F.col("snapshot.sensor_type"),
        F.col("snapshot.sensor_value"),
        F.col("snapshot.timestamp"),
        "dbx_ingest_date",
        "adls_upload_date",
        "source_file",
        "execution_id",
    )

    casted_sensors_df = flattened_sensors_df.select(
        F.col("metadata.device_id").alias("device_id"),
        F.col("metadata.test_id").alias("test_id"),
        F.try_to_timestamp("metadata.test_date", F.lit("yyyyMMdd_HHmmss")).alias("tested_at"),
        F.col("metadata.testing_software").alias("testing_software"),
        F.col("metadata.underlying_system").alias("underlying_system"),
        F.col("hardware_name").try_cast(StringType()).alias("hardware_name"),
        F.col("sensor_name").try_cast(StringType()).alias("sensor_name"),
        F.col("sensor_type").try_cast(StringType()).alias("sensor_type"),
        F.col("sensor_value").try_cast(DoubleType()).alias("sensor_value"),
        F.try_to_timestamp("timestamp", F.lit("yyyyMMdd_HHmmssSSS")).alias("polled_at"),
        F.col("dbx_ingest_date").alias("dbx_ingested_at"),
        "adls_upload_date",
        "source_file",
        "execution_id",
    )

    checked_casted_sensors_df = casted_sensors_df.withColumn(
        "quarantine",
        F.filter(
            F.array(
                F.when(F.col("tested_at").isNull(), "null_test_date"),
                F.when(F.col("hardware_name").isNull(), "null_hardware_name"),
                F.when(F.col("sensor_name").isNull(), "null_sensor_name"),
                F.when(F.col("sensor_type").isNull(), "null_sensor_type"),
                F.when(F.isnan("sensor_value"), "nan_sensor_value"),
                F.when(F.col("sensor_value").isNull(), "null_sensor_value"),
                F.when(F.col("sensor_value").isin(float("inf"), float("-inf")), "infinite_sensor_value"),
                F.when(F.col("polled_at").isNull(), "null_polled_at"),
            ),
            lambda reason: reason.isNotNull(),
        ),
    )

    quarantined_sensors_df = checked_casted_sensors_df.filter(F.size("quarantine") > 0)

    validated_sensors_df = checked_casted_sensors_df.filter(F.size("quarantine") == 0).drop("quarantine")

    expected_silver_schema = silver_schema_check(validated_sensors_df.schema["testing_software"].dataType)

    if set(validated_sensors_df.columns) == set(expected_silver_schema.fieldNames()):
        for col in validated_sensors_df.columns:
            if validated_sensors_df.schema[col].dataType != expected_silver_schema[col].dataType:
                raise ValueError(
                    f"Mismatch between dataTypes of validated_sensors_df and expected_silver_schema for column: {col}"
                )
    else:
        raise ValueError("Mismatch between expected columns in validated_sensors_df and expected_silver_schema")

    logger.info(f"Silver dataframe prepped for denormalization from bronze table: {config.source_table} ")
    return validated_sensors_df, (
        normalize_quarantined_df(quarantined_bronze_df, quarantine_batch_id),
        normalize_quarantined_df(quarantined_sensors_df, quarantine_batch_id),
    )


def denormalized_bronze_tables(cleaned_bronze_df: DataFrame):
    return


def write_to_silver(silver_dfs: DataFrame, quarantined_dfs: tuple[DataFrame, DataFrame]) -> None:
    return
