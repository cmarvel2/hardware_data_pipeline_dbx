import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

logger = logging.getLogger(__name__)

RESCUED_DATA_COLUMN: str = "rescued_data"


def add_ingestion_metadata(sdf: DataFrame, run_id: str) -> DataFrame:
    sdf = sdf.withColumns(
        {"source_file": col("_metadata.file_path"), "ingested_at": current_timestamp(), "execution_id": lit(run_id)}
    )
    logger.info("Columns source_file, ingested_at and execution_id added")
    return sdf
