from dataclasses import FrozenInstanceError

import pytest

from databricks_pipeline.bronze.config.bronze_config import BronzeConfig

pytestmark = pytest.mark.unit


@pytest.fixture
def config() -> BronzeConfig:
    return BronzeConfig(
        catalog="hardware_test",
        bronze_schema="bronze",
        landing_volume="hardware_landing",
        bronze_table="sensor_readings",
    )


def test_bronze_config_stores_supplied_values(config: BronzeConfig) -> None:
    assert config.catalog == "hardware_test"
    assert config.bronze_schema == "bronze"
    assert config.landing_volume == "hardware_landing"
    assert config.bronze_table == "sensor_readings"


def test_bronze_config_is_immutable(config: BronzeConfig) -> None:
    with pytest.raises(FrozenInstanceError):
        setattr(config, "catalog", "hardware_prod")  # noqa: B010


def test_bronze_config_builds_expected_volume_path(
    config: BronzeConfig,
) -> None:
    assert config.volume_path == ("/Volumes/hardware_test/bronze/hardware_landing")


def test_bronze_config_builds_expected_source_path(
    config: BronzeConfig,
) -> None:
    assert config.source_path == ("/Volumes/hardware_test/bronze/hardware_landing/incoming")


def test_bronze_config_builds_expected_pipeline_state_path(
    config: BronzeConfig,
) -> None:
    assert config.pipeline_state_path == (
        "/Volumes/hardware_test/bronze/hardware_landing/_pipeline_state/sensor_readings"
    )


def test_bronze_config_builds_expected_schema_location(
    config: BronzeConfig,
) -> None:
    assert config.schema_location == (
        "/Volumes/hardware_test/bronze/hardware_landing/_pipeline_state/sensor_readings/schema"
    )


def test_bronze_config_builds_expected_checkpoint_location(
    config: BronzeConfig,
) -> None:
    assert config.checkpoint_location == (
        "/Volumes/hardware_test/bronze/hardware_landing/_pipeline_state/sensor_readings/checkpoint"
    )


def test_pipeline_state_is_outside_monitored_source_tree(
    config: BronzeConfig,
) -> None:
    assert not config.pipeline_state_path.startswith(f"{config.source_path}/")


def test_bronze_config_builds_expected_target_table(
    config: BronzeConfig,
) -> None:
    assert config.target_table == "hardware_test.bronze.sensor_readings"
