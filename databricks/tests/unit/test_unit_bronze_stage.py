from dataclasses import dataclass
from unittest.mock import Mock, call

import pytest
from pytest_mock import MockerFixture

from databricks_pipeline.bronze.config.bronze_config import BronzeConfig
from databricks_pipeline.bronze.stage import bronze_stage

pytestmark = pytest.mark.unit


@dataclass(frozen=True)
class StageTestSetup:
    spark: Mock
    reader: Mock
    raw_dataframe: Mock
    enriched_dataframe: Mock
    writer: Mock
    query: Mock
    add_ingestion_metadata: Mock


@pytest.fixture
def config() -> BronzeConfig:
    return BronzeConfig(
        catalog="hardware_test",
        bronze_schema="bronze",
        landing_volume="hardware_landing",
        bronze_table="sensor_readings",
    )


@pytest.fixture
def stage_test_setup(mocker: MockerFixture) -> StageTestSetup:
    spark = Mock(name="spark")
    reader = Mock(name="autoloader_reader")
    raw_dataframe = Mock(name="raw_dataframe")
    enriched_dataframe = Mock(name="enriched_dataframe")
    writer = Mock(name="delta_stream_writer")
    query = Mock(name="streaming_query")

    spark.readStream.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = raw_dataframe

    enriched_dataframe.writeStream.format.return_value = writer
    writer.option.return_value = writer
    writer.trigger.return_value = writer
    writer.toTable.return_value = query

    add_ingestion_metadata = mocker.patch.object(
        bronze_stage,
        "add_ingestion_metadata",
        return_value=enriched_dataframe,
    )

    return StageTestSetup(
        spark=spark,
        reader=reader,
        raw_dataframe=raw_dataframe,
        enriched_dataframe=enriched_dataframe,
        writer=writer,
        query=query,
        add_ingestion_metadata=add_ingestion_metadata,
    )


def test_landing_hardware_autoloader_reads_source_with_expected_options(
    config: BronzeConfig,
    stage_test_setup: StageTestSetup,
) -> None:
    bronze_stage.landing_hardware_autoloader(
        spark=stage_test_setup.spark,
        config=config,
        run_id="run-123",
    )

    stage_test_setup.spark.readStream.format.assert_called_once_with("cloudFiles")
    stage_test_setup.reader.option.assert_has_calls(
        [
            call("cloudFiles.format", "json"),
            call("cloudFiles.schemaLocation", config.schema_location),
            call("cloudFiles.inferColumnTypes", "true"),
            call(
                "rescuedDataColumn",
                bronze_stage.RESCUED_DATA_COLUMN,
            ),
            call("cloudFiles.partitionColumns", "ingest_date"),
        ]
    )
    assert stage_test_setup.reader.option.call_count == 5
    stage_test_setup.reader.load.assert_called_once_with(config.source_path)


def test_landing_hardware_autoloader_adds_ingestion_metadata(
    config: BronzeConfig,
    stage_test_setup: StageTestSetup,
) -> None:
    bronze_stage.landing_hardware_autoloader(
        spark=stage_test_setup.spark,
        config=config,
        run_id="run-123",
    )

    stage_test_setup.add_ingestion_metadata.assert_called_once_with(
        sdf=stage_test_setup.raw_dataframe,
        run_id="run-123",
    )


def test_landing_hardware_autoloader_writes_enriched_dataframe_to_delta(
    config: BronzeConfig,
    stage_test_setup: StageTestSetup,
) -> None:
    bronze_stage.landing_hardware_autoloader(
        spark=stage_test_setup.spark,
        config=config,
        run_id="run-123",
    )

    (stage_test_setup.enriched_dataframe.writeStream.format.assert_called_once_with("delta"))
    stage_test_setup.writer.option.assert_has_calls(
        [
            call(
                "checkpointLocation",
                config.checkpoint_location,
            ),
            call("mergeSchema", "true"),
        ]
    )
    assert stage_test_setup.writer.option.call_count == 2
    stage_test_setup.writer.trigger.assert_called_once_with(availableNow=True)
    stage_test_setup.writer.toTable.assert_called_once_with(config.target_table)


def test_landing_hardware_autoloader_waits_for_query_to_finish(
    config: BronzeConfig,
    stage_test_setup: StageTestSetup,
) -> None:
    bronze_stage.landing_hardware_autoloader(
        spark=stage_test_setup.spark,
        config=config,
        run_id="run-123",
    )

    stage_test_setup.query.awaitTermination.assert_called_once_with()


def test_landing_hardware_autoloader_returns_none(
    config: BronzeConfig,
    stage_test_setup: StageTestSetup,
) -> None:
    result = bronze_stage.landing_hardware_autoloader(
        spark=stage_test_setup.spark,
        config=config,
        run_id="run-123",
    )

    assert result is None
