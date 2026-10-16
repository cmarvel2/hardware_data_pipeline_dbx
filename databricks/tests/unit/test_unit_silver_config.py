import pytest

from databricks_pipeline.silver.config.silver_config import SilverConfig

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("table", ["fct_sensors", "dim_hardware_tests", "dim_dbx_metadata"])
def test_silver_config_resolves_each_output(table: str) -> None:
    config = SilverConfig(
        catalog="hardware_test", bronze_schema="bronze", bronze_table="sensor_readings", silver_schema="silver"
    )
    assert config.source_table == "hardware_test.bronze.sensor_readings"
    assert config.target_table(table) == f"hardware_test.silver.{table}"
