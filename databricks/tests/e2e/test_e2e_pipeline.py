import copy
import json

import pytest
from pyspark.sql import SparkSession

from databricks_pipeline.bronze.config.bronze_config import BronzeConfig
from databricks_pipeline.bronze.stage.bronze_stage import landing_hardware_autoloader
from databricks_pipeline.common.ops_config import CommonConfig
from databricks_pipeline.gold.config.gold_config import GoldConfig
from databricks_pipeline.gold.stage.gold_stage import table_asus_proart_p16_h7606wv
from databricks_pipeline.silver.config.silver_config import SilverConfig
from databricks_pipeline.silver.stage.silver_stage import (
    bronze_data_cleaning,
    normalized_bronze_tables,
    write_incoming_quarantine,
    write_outgoing_quarantine,
    write_to_silver,
)

pytestmark = [pytest.mark.e2e, pytest.mark.integration, pytest.mark.databricks]


def test_landing_to_gold_routes_invalid_rows_and_restarts_without_duplicates(
    lakehouse: tuple[SparkSession, GoldConfig, str],
) -> None:
    spark, gold, bronze_schema = lakehouse
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    bronze = BronzeConfig(gold.catalog, bronze_schema, "landing", "sensor_readings")
    silver = SilverConfig(gold.catalog, bronze_schema, bronze.bronze_table, gold.silver_schema)
    operations = CommonConfig(gold.catalog, "operations", bronze_schema, "incoming_quarantine", "outgoing_quarantine")
    for volume in (bronze.landing_volume, operations.ops_volume):
        spark.sql(f"CREATE VOLUME {gold.catalog}.{bronze_schema}.{volume}")

    valid_payload = {
        "metadata": {
            "underlying_system": "LibreHardwareMonitor",
            "testing_software": {"device": "ASUS_ProArt_P16_(H7606)"},
            "device_id": "e2e-device",
            "test_id": "e2e-valid",
            "test_date": "20261019_120000",
        },
        "snapshots": [
            {
                "hardware_name": "CPU",
                "sensor_name": "CPU Power",
                "sensor_type": "Power",
                "sensor_value": value,
                "timestamp": timestamp,
            }
            for value, timestamp in (
                ("40.0", "20261019_120000000"),
                ("41.0", "20261019_121000000"),
                ("invalid", "20261019_121100000"),
            )
        ],
    }
    missing_device = copy.deepcopy(valid_payload)
    missing_device["metadata"]["device_id"] = None
    empty_payload = copy.deepcopy(valid_payload)
    empty_payload["snapshots"] = []
    lines = [(json.dumps(payload),) for payload in (valid_payload, missing_device, empty_payload)]
    spark.createDataFrame(lines, "value STRING").coalesce(1).write.text(f"{bronze.source_path}/ingest_date=2026-10-19")

    # The second run reuses the same checkpoints without introducing new files.
    for run_number in (1, 2):
        landing_hardware_autoloader(spark, bronze, operations, run_id=f"e2e-{run_number}")
        accepted, incoming, outgoing = bronze_data_cleaning(spark, silver)
        facts, tests, files = normalized_bronze_tables(accepted)
        write_incoming_quarantine(incoming, operations)
        write_outgoing_quarantine(outgoing, operations)
        for dataframe, name in ((facts, "fct_sensors"), (tests, "dim_hardware_tests"), (files, "dim_dbx_metadata")):
            write_to_silver(dataframe, silver, operations, name)
        table_asus_proart_p16_h7606wv(spark, gold)

        for table, count in (
            (bronze.target_table, 3),
            (silver.target_table("fct_sensors"), 2),
            (silver.target_table("dim_hardware_tests"), 1),
            (silver.target_table("dim_dbx_metadata"), 1),
            (operations.inc_quarantine_target_table, 2),
            (operations.out_quarantine_target_table, 1),
            (gold.target_table("mart_proart_p16_h7606wv"), 2),
        ):
            assert spark.table(table).count() == count, f"Unexpected row count for {table} on run {run_number}"

    reasons = {
        reason
        for row in spark.table(operations.inc_quarantine_target_table).select("quarantine").collect()
        for reason in row["quarantine"]
    }
    assert reasons == {"null_metadata_device_id", "empty_snapshots"}
    outgoing_row = spark.table(operations.out_quarantine_target_table).first()
    assert outgoing_row is not None
    assert outgoing_row["quarantine"] == ["null_sensor_value"]
    phases = spark.table(gold.target_table("mart_proart_p16_h7606wv")).select("thermal_rampup_phase").collect()
    assert sorted(row["thermal_rampup_phase"] for row in phases) == [False, True]
