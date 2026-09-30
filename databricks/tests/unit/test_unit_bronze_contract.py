from typing import TypedDict, cast
from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame
from pytest_mock import MockerFixture

from databricks_pipeline.bronze.schema import bronze_contract

pytestmark = pytest.mark.unit


class MetadataTestSetup(TypedDict):
    source_dataframe: Mock
    result_dataframe: Mock
    source_file_expression: Mock
    ingest_timestamp_expression: Mock
    execution_id_expression: Mock
    col: Mock
    current_timestamp: Mock
    lit: Mock


@pytest.fixture
def metadata_test_setup(mocker: MockerFixture) -> MetadataTestSetup:
    source_file_expression = mocker.Mock(name="source_file_expression")
    ingest_timestamp_expression = mocker.Mock(name="ingest_timestamp_expression")
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
        return_value=ingest_timestamp_expression,
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
        "ingest_timestamp_expression": ingest_timestamp_expression,
        "execution_id_expression": execution_id_expression,
        "col": col,
        "current_timestamp": current_timestamp,
        "lit": lit,
    }


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
            "dbx_ingest_date": metadata_test_setup["ingest_timestamp_expression"],
            "execution_id": metadata_test_setup["execution_id_expression"],
        }
    )


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


def test_add_ingestion_metadata_returns_enriched_dataframe(
    metadata_test_setup: MetadataTestSetup,
) -> None:
    result = bronze_contract.add_ingestion_metadata(
        cast(DataFrame, metadata_test_setup["source_dataframe"]),
        "run-123",
    )

    assert result is metadata_test_setup["result_dataframe"]
