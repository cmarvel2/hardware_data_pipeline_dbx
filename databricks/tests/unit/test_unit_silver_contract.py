import pytest
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    VariantType,
)

from databricks_pipeline.silver.schema.silver_contract import (
    EXPECTED_BRONZE_COLUMNS,
    SILVER_SCHEMA_CHECK,
    quarantine_schema_check,
)

pytestmark = pytest.mark.unit


def test_silver_contract_keeps_software_flexible_and_observations_typed() -> None:
    expected = {
        "device_id": StringType(),
        "test_id": StringType(),
        "tested_at": TimestampType(),
        "testing_software": VariantType(),
        "underlying_system": StringType(),
        "hardware_name": StringType(),
        "sensor_name": StringType(),
        "sensor_type": StringType(),
        "sensor_value": DoubleType(),
        "polled_at": TimestampType(),
        "dbx_ingested_at": TimestampType(),
        "adls_upload_date": DateType(),
        "source_file": StringType(),
        "execution_id": StringType(),
    }
    assert {field.name: field.dataType for field in SILVER_SCHEMA_CHECK} == expected


def test_bronze_contract_requires_the_actual_corrupt_record_name() -> None:
    assert "corrupt_record" in EXPECTED_BRONZE_COLUMNS
    assert "corrupted_data" not in EXPECTED_BRONZE_COLUMNS


@pytest.mark.parametrize("payload", ["bronze", "sensor"])
def test_quarantine_contract_preserves_single_struct_payload(payload: str) -> None:
    fields = (
        [StructField("metadata", StructType([StructField("testing_software", VariantType())]))]
        if payload == "bronze"
        else [StructField("sensor_value", DoubleType()), StructField("testing_software", VariantType())]
    )
    payload_type = StructType(fields)
    schema = quarantine_schema_check(payload_type)
    assert {field.name: field.dataType for field in schema} == {
        "quarantined_at": TimestampType(),
        "quarantine": ArrayType(StringType()),
        "quarantined_rows": payload_type,
        "quarantined_batch_id": StringType(),
    }
