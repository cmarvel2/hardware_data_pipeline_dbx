import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

logger = logging.getLogger(__name__)

RESCUED_DATA_COLUMN: str = "rescued_data"
CORRUPT_DATA_COLUMN: str = "corrupt_record"


def add_ingestion_metadata(sdf: DataFrame, run_id: str) -> DataFrame:
    sdf = sdf.withColumns(
        {"source_file": col("_metadata.file_path"), "dbx_ingest_date": current_timestamp(), "execution_id": lit(run_id)}
    )
    logger.info("Columns source_file, dbx_ingest_date and execution_id added")
    return sdf
