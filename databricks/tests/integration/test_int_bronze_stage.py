import datetime
import json
from typing import cast
from uuid import uuid4

import pytest
from pyspark.sql import SparkSession, functions as F  # noqa
from pyspark.sql.types import StructType, VariantType

from databricks_pipeline.bronze.config.bronze_config import BronzeConfig
from databricks_pipeline.bronze.stage.bronze_stage import (
    landing_hardware_autoloader,
)
from databricks_pipeline.common.ops_config import CommonConfig

pytestmark = [pytest.mark.integration, pytest.mark.databricks]


def test_landing_hardware_autoloader_ingests_json_into_test_bronze_table(
    spark: SparkSession,
    test_target: dict[str, object],
) -> None:
    variables = cast(dict[str, str], test_target["variables"])
    missing_ops = {"ops_schema", "ops_volume"} - variables.keys()
    assert not missing_ops, f"Configure these variables for the test bundle target: {sorted(missing_ops)}"

    config = BronzeConfig(
        catalog=variables["catalog"],
        bronze_schema=variables["bronze_schema"],
        landing_volume=variables["landing_volume"],
        bronze_table=variables["bronze_table"],
    )
    commonconfig = CommonConfig(
        catalog=variables["catalog"],
        ops_schema=variables["ops_schema"],
        ops_volume=variables["ops_volume"],
        incoming_quarantine_table=None,
        outgoing_quarantine_table=None,
    )

    test_id = uuid4().hex
    execution_id = f"integration-{test_id}"
    device_id = f"integration-device-{test_id}"
    ingest_date = datetime.datetime.now(tz=datetime.UTC).date().isoformat()

    payload_json = json.dumps(
        {
            "metadata": {
                "underlying_system": "LibreHardwareMonitor",
                "device_id": device_id,
                "test_id": f"integration-test-{test_id}",
                "test_date": "20260913_120000",
                "testing_software": {"integration_tool": {"profile": "sustained"}},
            },
            "snapshots": [
                {
                    "timestamp": "20260913_120000000",
                    "hardware_name": "Integration CPU /cpu/0",
                    "sensor_name": "CPU Total",
                    "sensor_type": "Load",
                    "sensor_value": 45.2,
                },
            ],
        }
    )

    input_directory = f"{config.source_path}/ingest_date={ingest_date}"

    (spark.createDataFrame([(payload_json,)], ["value"]).coalesce(1).write.mode("append").text(input_directory))

    landing_hardware_autoloader(
        spark=spark,
        config=config,
        commonconfig=commonconfig,
        run_id=execution_id,
    )

    assert spark.catalog.tableExists(config.target_table), f"Bronze table was not created: {config.target_table}"

    matching_rows = (
        spark.table(config.target_table)
        .where(F.col("execution_id") == execution_id)
        .where(F.col("metadata.device_id") == device_id)
        .select(
            "adls_upload_date",
            "source_file",
            "dbx_ingest_date",
            "execution_id",
            "metadata",
            "snapshots",
            "corrupt_record",
        )
    )

    assert matching_rows.count() == 1

    row = matching_rows.first()

    assert row is not None
    assert str(row["adls_upload_date"]) == ingest_date
    assert row["source_file"] is not None
    assert row["dbx_ingest_date"] is not None
    assert row["execution_id"] == execution_id
    assert row["metadata"]["device_id"] == device_id
    assert len(row["snapshots"]) == 1
    assert row["corrupt_record"] is None
    metadata_type = cast(StructType, matching_rows.schema["metadata"].dataType)
    assert isinstance(metadata_type["testing_software"].dataType, VariantType)
