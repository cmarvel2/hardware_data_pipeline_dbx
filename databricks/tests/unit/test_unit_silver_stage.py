from dataclasses import dataclass
from typing import Any, cast
from unittest.mock import Mock
from uuid import UUID

import pytest
from google.protobuf.json_format import MessageToDict
from pyspark.sql import Column, DataFrame
from pyspark.sql.connect import functions as connect_functions
from pyspark.sql.connect.column import Column as ConnectColumn
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, VariantType
from pytest_mock import MockerFixture

from databricks_pipeline.common.ops_config import CommonConfig
from databricks_pipeline.silver.config.silver_config import SilverConfig
from databricks_pipeline.silver.schema.silver_contract import SILVER_SCHEMA_CHECK, quarantine_schema_check
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
    )


@dataclass(frozen=True)
class QuarantineSetup:
    source: Mock
    packaged: Mock
    output: Mock


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
    output.count.return_value = 2
    return QuarantineSetup(source, packaged, output)


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
    validated.schema = SILVER_SCHEMA_CHECK
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
    normalize.return_value = Mock(name="normalized_rows")
    mocker.patch.object(silver_stage.uuid, "uuid4", return_value=UUID(BATCH_ID))
    return CleaningSetup(
        spark,
        bronze,
        checked_bronze,
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
    normalized_bronze = Mock(name="normalized_bronze")
    normalized_sensors = Mock(name="normalized_sensors")
    cleaning_setup.normalize.side_effect = [normalized_bronze, normalized_sensors]
    validated, incoming, outgoing = silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)

    cleaning_setup.spark.readStream.table.assert_called_once_with(config.source_table)
    assert validated is cleaning_setup.validated
    assert incoming is normalized_bronze
    assert outgoing is normalized_sensors
    assert cleaning_setup.normalize.call_args_list[0].args == (cleaning_setup.bronze_quarantine, BATCH_ID)
    assert cleaning_setup.normalize.call_args_list[1].args == (cleaning_setup.sensor_quarantine, BATCH_ID)


@pytest.mark.parametrize(
    ("column", "reason"),
    [
        ("metadata", "null_metadata"),
        ("metadata.device_id", "null_metadata_device_id"),
        ("metadata.test_date", "null_metadata_test_date"),
        ("metadata.test_id", "null_metadata_test_id"),
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


def test_cleaning_accepts_nullable_schema_with_variant_software_metadata(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
) -> None:
    cleaning_setup.validated.schema = StructType(
        [StructField(field.name, field.dataType, True) for field in SILVER_SCHEMA_CHECK]
    )
    validated, _, _ = silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    assert validated is cleaning_setup.validated
    assert validated.schema["testing_software"].dataType == VariantType()


def test_cleaning_checks_sql_null_and_variant_null_software(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
) -> None:
    silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)
    conditions = reason_conditions(cleaning_setup.bronze.withColumn.call_args.args[1])
    software = connect_functions.col("metadata.testing_software")
    assert conditions["null_metadata_testing_software"] == expression_plan(
        software.isNull() | connect_functions.is_variant_null(software)
    )


def test_cleaning_rejects_struct_when_software_contract_requires_variant(
    config: SilverConfig,
    cleaning_setup: CleaningSetup,
) -> None:
    cleaning_setup.validated.schema = StructType(
        [
            StructField(field.name, StructType([]) if field.name == "testing_software" else field.dataType)
            for field in SILVER_SCHEMA_CHECK
        ]
    )
    with pytest.raises(ValueError, match="testing_software"):
        silver_stage.bronze_data_cleaning(cleaning_setup.spark, config)


@dataclass(frozen=True)
class NormalizationSetup:
    source: Mock
    projections: list[Mock]
    deduplicated: list[Mock]


@pytest.fixture
def normalization_setup() -> NormalizationSetup:
    source = Mock(name="cleaned_stream")
    source.isStreaming = True
    projections = [Mock(name=name) for name in ("sensors", "tests", "metadata")]
    deduplicated = [Mock(name=f"unique_{name}") for name in ("sensors", "tests", "metadata")]
    source.select.side_effect = projections
    for projected, unique in zip(projections, deduplicated, strict=True):
        projected.dropDuplicates.return_value = unique
    return NormalizationSetup(source, projections, deduplicated)


def test_normalization_projects_three_outputs_with_their_natural_keys(
    normalization_setup: NormalizationSetup,
) -> None:
    result = silver_stage.normalized_bronze_tables(cast(DataFrame, normalization_setup.source))
    assert result == tuple(normalization_setup.deduplicated)
    assert [call.args for call in normalization_setup.source.select.call_args_list] == [
        ("test_id", "hardware_name", "sensor_name", "sensor_type", "sensor_value", "polled_at", "source_file"),
        ("test_id", "tested_at", "device_id", "testing_software", "underlying_system"),
        ("source_file", "adls_upload_date", "execution_id", "dbx_ingested_at"),
    ]
    for projected, keys in zip(
        normalization_setup.projections,
        [
            ["test_id", "hardware_name", "sensor_name", "sensor_type", "polled_at"],
            ["test_id"],
            ["source_file"],
        ],
        strict=True,
    ):
        projected.dropDuplicates.assert_called_once_with(keys)


@pytest.mark.parametrize("output", ["incoming", "outgoing", "fct_sensors"])
@pytest.mark.parametrize("failure_at", [None, "start", "await"])
def test_streaming_writers_use_distinct_paths_and_propagate_failures(
    config: SilverConfig,
    commonconfig: CommonConfig,
    output: str,
    failure_at: str | None,
) -> None:
    dataframe = Mock(name="streaming_output")
    writer = dataframe.writeStream.format.return_value
    writer.outputMode.return_value = writer
    writer.option.return_value = writer
    writer.trigger.return_value = writer
    query = writer.toTable.return_value
    failure = RuntimeError("Delta write failed")
    if failure_at == "start":
        writer.toTable.side_effect = failure
    elif failure_at == "await":
        query.awaitTermination.side_effect = failure

    if output in ("incoming", "outgoing"):
        target = (
            commonconfig.inc_quarantine_target_table
            if output == "incoming"
            else commonconfig.out_quarantine_target_table
        )
        checkpoint = commonconfig.checkpoint_location(target)
    else:
        target = config.target_table(output)
        checkpoint = commonconfig.checkpoint_location(output)

    def invoke() -> None:
        if output == "incoming":
            silver_stage.write_incoming_quarantine(cast(DataFrame, dataframe), commonconfig)
        elif output == "outgoing":
            silver_stage.write_outgoing_quarantine(cast(DataFrame, dataframe), commonconfig)
        else:
            silver_stage.write_to_silver(cast(DataFrame, dataframe), config, commonconfig, output)

    if failure_at is None:
        invoke()
    else:
        with pytest.raises(RuntimeError, match="Delta write failed") as caught:
            invoke()
        assert caught.value is failure

    dataframe.writeStream.format.assert_called_once_with("delta")
    writer.outputMode.assert_called_once_with("append")
    writer.option.assert_any_call("checkpointLocation", checkpoint)
    writer.trigger.assert_called_once_with(availableNow=True)
    writer.toTable.assert_called_once_with(target)
    if failure_at == "start":
        query.awaitTermination.assert_not_called()
    else:
        query.awaitTermination.assert_called_once_with()
