import datetime
import json
from typing import cast
from uuid import uuid4

import pytest
from pyspark.sql import SparkSession, functions as F  #noqa

from databricks_pipeline.bronze.config.bronze_config import BronzeConfig
from databricks_pipeline.bronze.stage.bronze_stage import (
    landing_hardware_autoloader,
)

pytestmark = [pytest.mark.integration, pytest.mark.databricks]


def test_landing_hardware_autoloader_ingests_json_into_test_bronze_table(
    spark: SparkSession,
    test_target: dict[str, object],
) -> None:
    variables = cast(dict[str, str], test_target["variables"])

    config = BronzeConfig(
        catalog=variables["catalog"],
        bronze_schema=variables["bronze_schema"],
        landing_volume=variables["landing_volume"],
        bronze_table=variables["bronze_table"],
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
        run_id=execution_id,
    )

    assert spark.catalog.tableExists(config.target_table), f"Bronze table was not created: {config.target_table}"

    matching_rows = (
        spark.table(config.target_table)
        .where(F.col("execution_id") == execution_id)
        .where(F.col("metadata.device_id") == device_id)
        .select(
            "source_file",
            "ingested_at",
            "execution_id",
            "metadata",
            "snapshots",
        )
    )

    assert matching_rows.count() == 1

    row = matching_rows.first()

    assert row is not None
    assert row["execution_id"] == execution_id
    assert row["source_file"] is not None
    assert row["ingested_at"] is not None
    assert row["metadata"]["device_id"] == device_id
    assert len(row["snapshots"]) == 1
