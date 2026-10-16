import pytest

from databricks_pipeline.common.ops_config import CommonConfig

pytestmark = pytest.mark.unit


def test_ops_volume_is_separate_from_landing(commonconfig: CommonConfig) -> None:
    assert commonconfig.ops_volume_path == "/Volumes/hardware_test/ops/pipeline_operations"
    assert not commonconfig.pipeline_state_path("sensor_readings").startswith(
        "/Volumes/hardware_test/bronze/hardware_landing/incoming/"
    )


@pytest.mark.parametrize("query_id", ["sensor_readings", "silver/fct_sensors", "silver/dim_hardware_tests"])
def test_ops_paths_are_stable_and_scoped_to_query(commonconfig: CommonConfig, query_id: str) -> None:
    state = f"/Volumes/hardware_test/ops/pipeline_operations/_pipeline_state/{query_id}"
    assert commonconfig.pipeline_state_path(query_id) == state
    assert commonconfig.schema_location(query_id) == f"{state}/schema"
    assert commonconfig.checkpoint_location(query_id) == f"{state}/checkpoint"


def test_quarantine_targets_and_checkpoints_are_distinct(commonconfig: CommonConfig) -> None:
    assert commonconfig.inc_quarantine_target_table == "hardware_test.ops.incoming_silver_quarantine"
    assert commonconfig.out_quarantine_target_table == "hardware_test.ops.outgoing_silver_quarantine"
    assert commonconfig.checkpoint_location(commonconfig.inc_quarantine_target_table) != (
        commonconfig.checkpoint_location(commonconfig.out_quarantine_target_table)
    )
