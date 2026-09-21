from typing import TypedDict, cast
from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame
from pytest_mock import MockerFixture

from databricks_pipeline.bronze.schema import bronze_contract


class MetadataTestSetup(TypedDict):
    source_dataframe: Mock
    result_dataframe: Mock
    source_file_expression: Mock
    ingested_at_expression: Mock
    execution_id_expression: Mock
    col: Mock
    current_timestamp: Mock
    lit: Mock


@pytest.fixture
def metadata_test_setup(
    mocker: MockerFixture,
) -> MetadataTestSetup:
    source_file_expression = mocker.Mock(name="source_file_expression")
    ingested_at_expression = mocker.Mock(name="ingested_at_expression")
    execution_id_expression = mocker.Mock(name="execution_id_expression")

    source_dataframe = mocker.Mock(name="source_dataframe")
    result_dataframe = mocker.Mock(name="result_dataframe")
    source_dataframe.withColumns.return_value = result_dataframe

    col = mocker.patch.object(
        bronze_contract,
        "col",
        return_value=source_file_expression,
    )
    current_timestamp = mocker.patch.object(
        bronze_contract,
        "current_timestamp",
        return_value=ingested_at_expression,
    )
    lit = mocker.patch.object(
        bronze_contract,
        "lit",
        return_value=execution_id_expression,
    )

    return {
        "source_dataframe": source_dataframe,
        "result_dataframe": result_dataframe,
        "source_file_expression": source_file_expression,
        "ingested_at_expression": ingested_at_expression,
        "execution_id_expression": execution_id_expression,
        "col": col,
        "current_timestamp": current_timestamp,
        "lit": lit,
    }


@pytest.mark.unit
def test_add_ingestion_metadata_creates_required_audit_columns(
    metadata_test_setup: MetadataTestSetup,
) -> None:
    source_dataframe = metadata_test_setup["source_dataframe"]

    bronze_contract.add_ingestion_metadata(
        cast(DataFrame, source_dataframe),
        "run-123",
    )

    source_dataframe.withColumns.assert_called_once_with(
        {
            "source_file": metadata_test_setup["source_file_expression"],
            "ingested_at": metadata_test_setup["ingested_at_expression"],
            "execution_id": metadata_test_setup["execution_id_expression"],
        }
    )


@pytest.mark.unit
def test_add_ingestion_metadata_uses_spark_audit_expressions(
    metadata_test_setup: MetadataTestSetup,
) -> None:
    bronze_contract.add_ingestion_metadata(
        cast(DataFrame, metadata_test_setup["source_dataframe"]),
        "run-123",
    )

    metadata_test_setup["col"].assert_called_once_with("_metadata.file_path")
    metadata_test_setup["current_timestamp"].assert_called_once_with()
    metadata_test_setup["lit"].assert_called_once_with("run-123")


@pytest.mark.unit
def test_add_ingestion_metadata_returns_enriched_dataframe(
    metadata_test_setup: MetadataTestSetup,
) -> None:
    result = bronze_contract.add_ingestion_metadata(
        cast(DataFrame, metadata_test_setup["source_dataframe"]),
        "run-123",
    )

    assert result is metadata_test_setup["result_dataframe"]