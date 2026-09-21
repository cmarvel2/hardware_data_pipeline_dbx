import logging

from pyspark.sql import SparkSession

from databricks_pipeline.bronze.config.bronze_config import BronzeConfig
from databricks_pipeline.bronze.schema.bronze_contract import RESCUED_DATA_COLUMN, add_ingestion_metadata

logger = logging.getLogger(__name__)


def landing_hardware_autoloader(spark: SparkSession, config: BronzeConfig, run_id: str) -> None:
    sdf = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", config.schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", RESCUED_DATA_COLUMN)
        .option("cloudFiles.partitionColumns", "ingest_date")
        .load(config.source_path)
    )

    sdf = add_ingestion_metadata(sdf=sdf, run_id=run_id)

    (
        sdf.writeStream.format("delta")
        .option("checkpointLocation", config.checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(config.target_table)
    ).awaitTermination()
    logger.info(f"Auto loader ingestion completed to target table: {config.target_table} for run id: {run_id}")
