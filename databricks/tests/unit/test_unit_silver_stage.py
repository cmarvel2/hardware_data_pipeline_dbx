from dataclasses import dataclass
from typing import Any, cast
from unittest.mock import Mock
from uuid import UUID

import pytest
from google.protobuf.json_format import MessageToDict
from pyspark.sql import Column, DataFrame
from pyspark.sql.connect import functions as connect_functions
from pyspark.sql.connect.column import Column as ConnectColumn
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType
from pytest_mock import MockerFixture

from databricks_pipeline.silver.config.silver_config import SilverConfig
from databricks_pipeline.silver.schema.silver_contract import quarantine_schema_check, silver_schema_check
from databricks_pipeline.silver.stage import silver_stage

pytestmark = pytest.mark.unit

BATCH_ID = "00000000-0000-0000-0000-000000000042"


def expression_plan(expression: Column) -> dict[str, Any]:
    if not isinstance(expression, ConnectColumn):
        raise TypeError("Expected a Spark Connect Column")

    return MessageToDict(
        expression.to_plan(Mock()),
        preserving_proto_field_name=True,
    )


def reason_conditions(expression: Column) -> dict[str, dict[str, Any]]:
    reasons = expression_plan(expression)["unresolved_function"]["arguments"][0]
    conditions = {}
    for reason in reasons["unresolved_function"]["arguments"]:
        condition, message = reason["unresolved_function"]["arguments"]
        conditions[message["literal"]["string"]] = condition
    return conditions


@pytest.fixture(autouse=True)
def offline_expressions(mocker: MockerFixture) -> None:
    mocker.patch.object(silver_stage, "F", connect_functions)


@pytest.fixture
def config() -> SilverConfig:
    return SilverConfig(
        catalog="hardware_test",
        bronze_schema="bronze",
        bronze_table="sensor_readings",
        silver_schema="silver",
        silver_table="sensor_readings",
        quarantine_table="quarantine_record",
    )


@dataclass(frozen=True)
class QuarantineSetup:
    source: Mock
    packaged: Mock
    output: Mock
    payload_type: StructType


@pytest.fixture
def quarantine_setup() -> QuarantineSetup:
    payload_type = StructType(
        [
            StructField("sensor_value", DoubleType()),
            StructField("source_file", StringType()),
            StructField("execution_id", StringType()),
        ]
    )
    source = Mock(name="rejected_rows")
    source.columns = ["quarantine", *payload_type.fieldNames()]
    packaged = Mock(name="packaged_rows")
    output = Mock(name="normalized_rows")
    source.select.return_value = packaged
    packaged.withColumns.return_value = output
    output.schema = quarantine_schema_check(payload_type)
    output.columns = output.schema.fieldNames()
    return QuarantineSetup(source, packaged, output, payload_type)


def test_normalize_packages_each_row_and_keeps_reasons_outside_payload(
    quarantine_setup: QuarantineSetup,
) -> None:
    result = silver_stage.normalize_quarantined_df(cast(DataFrame, quarantine_setup.source), BATCH_ID)

    assert result is quarantine_setup.output
    quarantine_setup.source.select.assert_called_once()
    reason_name, payload, timestamp = quarantine_setup.source.select.call_args.args
    assert reason_name == "quarantine"
    alias = expression_plan(payload)["alias"]
    assert alias["name"] == ["quarantined_rows"]
    fields = alias["expr"]["unresolved_function"]
    assert fields["function_name"] == "struct"
    assert [field["unresolved_attribute"]["unparsed_identifier"] for field in fields["arguments"]] == [
        "sensor_value",
        "source_file",
        "execution_id",
    ]
    timestamp_alias = expression_plan(timestamp)["alias"]
    assert timestamp_alias["name"] == ["quarantined_at"]
    assert timestamp_alias["expr"]["unresolved_function"]["function_name"] == "current_timestamp"
    quarantine_setup.source.groupBy.assert_not_called()


def test_normalize_uses_supplied_batch_id_as_literal(quarantine_setup: QuarantineSetup) -> None:
    silver_stage.normalize_quarantined_df(cast(DataFrame, quarantine_setup.source), BATCH_ID)

    added = quarantine_setup.packaged.withColumns.call_args.args[0]
    assert expression_plan(added["quarantined_batch_id"]) == {"literal": {"string": BATCH_ID}}


def test_normalize_accepts_nullable_fields(quarantine_setup: QuarantineSetup) -> None:
    quarantine_setup.output.schema = StructType(
        [StructField(field.name, field.dataType, True) for field in quarantine_setup.output.schema]
    )
    assert silver_stage.normalize_quarantined_df(cast(DataFrame, quarantine_setup.source), BATCH_ID) is (
        quarantine_setup.output
    )


@pytest.mark.parametrize("fault", ["missing_column", "extra_column", "wrong_type"])
def test_normalize_rejects_incompatible_outer_schema(quarantine_setup: QuarantineSetup, fault: str) -> None:
    if fault == "missing_column":
        quarantine_setup.output.columns.remove("quarantined_at")
    elif fault == "extra_column":
        quarantine_setup.output.columns.append("unexpected")
    else:
        quarantine_setup.output.schema = StructType(
            [
                StructField(field.name, StringType() if field.name == "quarantined_at" else field.dataType)
                for field in quarantine_setup.output.schema
            ]
        )

    match = "quarantined_at" if fault == "wrong_type" else "Mismatch between expected columns"
    with pytest.raises(ValueError, match=match):
        silver_stage.normalize_quarantined_df(cast(DataFrame, quarantine_setup.source), BATCH_ID)


@dataclass(frozen=True)
class CleaningSetup:
    spark: Mock
    bronze: Mock
    checked_bronze: Mock
    exploded: Mock
    flattened: Mock
    casted: Mock
    checked_sensors: Mock
    validated: Mock
    bronze_quarantine: Mock
    sensor_quarantine: Mock
    normalize: Mock


@pytest.fixture
def cleaning_setup(mocker: MockerFixture) -> CleaningSetup:
    spark = Mock(name="spark")
    bronze = Mock(name="bronze")
    checked_bronze = Mock(name="checked_bronze")
    accepted_bronze = Mock(name="accepted_bronze")
    exploded = Mock(name="exploded")
    flattened = Mock(name="flattened")
    casted = Mock(name="casted")
    checked_sensors = Mock(name="checked_sensors")
    accepted_sensors = Mock(name="accepted_sensors")
    validated = Mock(name="validated")
    bronze_quarantine = Mock(name="bronze_quarantine")
    sensor_quarantine = Mock(name="sensor_quarantine")
    bronze.columns = sorted(silver_stage.EXPECTED_BRONZE_COLUMNS)
    software_type = StructType([StructField("tool", StringType())])
    validated.schema = silver_schema_check(software_type)
    validated.columns = validated.schema.fieldNames()
    spark.readStream.table.return_value = bronze
    bronze.withColumn.return_value = checked_bronze
    checked_bronze.filter.side_effect = [bronze_quarantine, accepted_bronze]
    accepted_bronze.withColumn.return_value = exploded
    exploded.select.return_value = flattened
    flattened.select.return_value = casted
    casted.withColumn.return_value = checked_sensors
    checked_sensors.filter.side_effect = [sensor_quarantine, accepted_sensors]
    accepted_sensors.drop.return_value = validated
    normalize = mocker.patch.object(silver_stage, "normalize_quarantined_df")
    normalize.side_effect = [Mock(name="normalized_bronze"), Mock(name="normalized_sensors")]
    mocker.patch.object(silver_stage.uuid, "uuid4", return_value=UUID(BATCH_ID))
    return CleaningSetup(
        spark,
        bronze,
        checked_bronze,
        exploded,
        flattened,
        casted,
        checked_sensors,
        validated,
        bronze_quarantine,
        sensor_quarantine,
        normalize,
    )


@pytest.mark.parametrize("missing", sorted(silver_stage.EXPECTED_BRONZE_COLUMNS))
def test_cleaning_rejects_missing_bronze_columns(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
    missing: str,
) -> None:
    cleaning_setup.bronze.columns.remove(missing)
    with pytest.raises(ValueError, match=missing):
        silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    cleaning_setup.bronze.withColumn.assert_not_called()
    cleaning_setup.normalize.assert_not_called()


def test_cleaning_reads_configured_source_and_returns_three_outputs(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
) -> None:
    validated, quarantines = silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)

    cleaning_setup.spark.readStream.table.assert_called_once_with(config.source_table)
    assert validated is cleaning_setup.validated
    assert len(quarantines) == 2
    assert cleaning_setup.normalize.call_args_list[0].args == (cleaning_setup.bronze_quarantine, BATCH_ID)
    assert cleaning_setup.normalize.call_args_list[1].args == (cleaning_setup.sensor_quarantine, BATCH_ID)


@pytest.mark.parametrize(
    ("column", "reason"),
    [
        ("metadata", "null_metadata"),
        ("metadata.device_id", "null_metadata_device_id"),
        ("metadata.test_date", "null_metadata_test_date"),
        ("metadata.test_id", "null_metadata_test_id"),
        ("metadata.testing_software", "null_metadata_testing_software"),
        ("metadata.underlying_system", "null_metadata_underlying_system"),
        ("snapshots", "null_snapshots"),
        ("source_file", "null_source_file"),
        ("execution_id", "null_execution_id"),
        ("dbx_ingest_date", "null_dbx_ingest_date"),
        ("adls_upload_date", "null_adls_upload_date"),
    ],
)
def test_cleaning_checks_required_payload_values(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
    column: str,
    reason: str,
) -> None:
    silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    conditions = reason_conditions(cleaning_setup.bronze.withColumn.call_args.args[1])
    assert conditions[reason] == expression_plan(connect_functions.col(column).isNull())


def test_cleaning_rejects_empty_rescued_and_corrupt_payloads(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
) -> None:
    silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    conditions = reason_conditions(cleaning_setup.bronze.withColumn.call_args.args[1])
    assert conditions["empty_snapshots"] == expression_plan(connect_functions.size("snapshots") == 0)
    for column, reason in [("rescued_data", "rescued_data_present"), ("corrupt_record", "corrupt_record_present")]:
        assert conditions[reason] == expression_plan(connect_functions.col(column).isNotNull())


def test_cleaning_routes_rejected_and_accepted_rows_by_reason_count(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
) -> None:
    silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    for checked in (cleaning_setup.checked_bronze, cleaning_setup.checked_sensors):
        rejected, accepted = checked.filter.call_args_list
        assert expression_plan(rejected.args[0]) == expression_plan(connect_functions.size("quarantine") > 0)
        assert expression_plan(accepted.args[0]) == expression_plan(connect_functions.size("quarantine") == 0)


def test_cleaning_parses_timestamps_from_existing_columns(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
) -> None:
    silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    projections = {
        plan["alias"]["name"][0]: plan["alias"]["expr"]
        for column in cleaning_setup.flattened.select.call_args.args
        if not isinstance(column, str)
        for plan in [expression_plan(column)]
    }
    for name, source, pattern in (
        ("tested_at", "metadata.test_date", "yyyyMMdd_HHmmss"),
        ("polled_at", "timestamp", "yyyyMMdd_HHmmssSSS"),
    ):
        assert projections[name] == expression_plan(
            connect_functions.try_to_timestamp(source, connect_functions.lit(pattern))
        )


@pytest.mark.parametrize(
    ("column", "reason"),
    [
        ("tested_at", "null_test_date"),
        ("hardware_name", "null_hardware_name"),
        ("sensor_name", "null_sensor_name"),
        ("sensor_type", "null_sensor_type"),
        ("sensor_value", "null_sensor_value"),
        ("polled_at", "null_polled_at"),
    ],
)
def test_cleaning_checks_nulls_on_projected_sensor_columns(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
    column: str,
    reason: str,
) -> None:
    silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    conditions = reason_conditions(cleaning_setup.casted.withColumn.call_args.args[1])
    assert conditions[reason] == expression_plan(connect_functions.col(column).isNull())


def test_cleaning_rejects_nan_and_infinite_readings(config: SilverConfig, cleaning_setup: CleaningSetup) -> None:
    silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    conditions = reason_conditions(cleaning_setup.casted.withColumn.call_args.args[1])
    assert conditions["nan_sensor_value"] == expression_plan(connect_functions.isnan("sensor_value"))
    assert conditions["infinite_sensor_value"] == expression_plan(
        connect_functions.col("sensor_value").isin(float("inf"), float("-inf"))
    )


@pytest.mark.parametrize("fault", ["missing_column", "extra_column", "wrong_type"])
def test_cleaning_rejects_incompatible_silver_schema(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
    fault: str,
) -> None:
    if fault == "missing_column":
        cleaning_setup.validated.columns.remove("sensor_value")
    elif fault == "extra_column":
        cleaning_setup.validated.columns.append("quarantine")
    else:
        cleaning_setup.validated.schema = StructType(
            [
                StructField(field.name, StringType() if field.name == "sensor_value" else field.dataType)
                for field in cleaning_setup.validated.schema
            ]
        )

    match = "sensor_value" if fault == "wrong_type" else "Mismatch between expected columns"
    with pytest.raises(ValueError, match=match):
        silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    cleaning_setup.normalize.assert_not_called()


def test_cleaning_propagates_read_failure(config: SilverConfig, cleaning_setup: CleaningSetup) -> None:
    failure = RuntimeError("Bronze table unavailable")
    cleaning_setup.spark.readStream.table.side_effect = failure
    with pytest.raises(RuntimeError, match="Bronze table unavailable") as caught:
        silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    assert caught.value is failure


def test_cleaning_accepts_nullable_schema_and_variable_software_metadata(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
) -> None:
    software_type = StructType([StructField("settings", ArrayType(StringType()))])
    cleaning_setup.validated.schema = StructType(
        [StructField(field.name, field.dataType, True) for field in silver_schema_check(software_type)]
    )
    validated, _ = silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    assert validated is cleaning_setup.validated
    assert validated.schema["testing_software"].dataType == software_type
