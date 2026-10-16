from pyspark.sql.types import (
    ArrayType,
    DataType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    VariantType,
)

EXPECTED_BRONZE_COLUMNS = frozenset(
    {
        "metadata",
        "snapshots",
        "adls_upload_date",
        "rescued_data",
        "source_file",
        "corrupt_record",
        "dbx_ingest_date",
        "execution_id",
    }
)


SILVER_SCHEMA_CHECK = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("test_id", StringType(), False),
        StructField("tested_at", TimestampType(), False),
        StructField("testing_software", VariantType(), False),
        StructField("underlying_system", StringType(), False),
        StructField("hardware_name", StringType(), False),
        StructField("sensor_name", StringType(), False),
        StructField("sensor_type", StringType(), False),
        StructField("sensor_value", DoubleType(), False),
        StructField("polled_at", TimestampType(), False),
        StructField("dbx_ingested_at", TimestampType(), False),
        StructField("adls_upload_date", DateType(), False),
        StructField("source_file", StringType(), False),
        StructField("execution_id", StringType(), False),
    ]
)


def quarantine_schema_check(quarantined_rows_array: DataType) -> StructType:
    return StructType(
        [
            StructField("quarantined_at", TimestampType(), False),
            StructField("quarantine", ArrayType(StringType()), False),
            StructField("quarantined_rows", quarantined_rows_array, False),
            StructField("quarantined_batch_id", StringType(), False),
        ]
    )
