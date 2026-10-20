import pytest

from databricks_pipeline.gold.config.gold_config import GoldConfig

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("catalog", ["hardware_dev", "hardware_test", "hardware_prod"])
def test_gold_config_qualifies_sources_and_targets(catalog: str) -> None:
    config = GoldConfig(catalog=catalog, silver_schema="silver", gold_schema="gold")
    assert config.source_table("fct_sensors") == f"{catalog}.silver.fct_sensors"
    assert config.source_table("dim_hardware_tests") == f"{catalog}.silver.dim_hardware_tests"
    assert config.target_table("mart_proart_p16_h7606wv") == f"{catalog}.gold.mart_proart_p16_h7606wv"
