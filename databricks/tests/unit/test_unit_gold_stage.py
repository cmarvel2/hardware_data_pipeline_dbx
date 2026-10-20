import logging
from pathlib import Path
from typing import cast
from unittest.mock import Mock

import pytest
import yaml
from pyspark.sql import SparkSession

from databricks_pipeline.gold.config.gold_config import GoldConfig
from databricks_pipeline.gold.stage import gold_stage

pytestmark = pytest.mark.unit


@pytest.fixture
def goldconfig() -> GoldConfig:
    return GoldConfig(catalog="hardware_test", silver_schema="silver", gold_schema="gold")


@pytest.fixture
def gold_query(goldconfig: GoldConfig) -> str:
    spark = Mock(spec=SparkSession)
    result = gold_stage.table_asus_proart_p16_h7606wv(cast(SparkSession, spark), goldconfig)
    assert result is None
    spark.sql.assert_called_once()
    return " ".join(spark.sql.call_args.args[0].split())


def test_gold_replaces_device_reporting_table(gold_query: str) -> None:
    assert gold_query.startswith("CREATE OR REPLACE TABLE hardware_test.gold.mart_proart_p16_h7606wv AS SELECT ")
    assert "ht.testing_software:device::STRING = 'ASUS_ProArt_P16_(H7606)'" in gold_query


def test_gold_sources_match_bundle_table_names(gold_query: str) -> None:
    bundle_path = Path(__file__).resolve().parents[2] / "databricks.yml"
    bundle = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    variables = bundle["targets"]["dev"]["variables"]
    source_schema = f"hardware_test.{variables['silver_schema']}"
    assert f"FROM {source_schema}.{variables['silver_sensors_table']} s" in gold_query
    assert f"JOIN {source_schema}.{variables['silver_tests_table']} ht ON s.test_id = ht.test_id" in gold_query


def test_gold_submits_per_test_ten_minute_rampup_expression(gold_query: str) -> None:
    assert (
        "case when s.polled_at < min(s.polled_at) over (partition by s.test_id) "
        "+ interval '10' minutes then true else false end as thermal_rampup_phase"
    ) in gold_query.lower()


def test_gold_propagates_sql_failure_without_logging_success(
    caplog: pytest.LogCaptureFixture, goldconfig: GoldConfig
) -> None:
    spark = Mock(spec=SparkSession)
    failure = RuntimeError("Gold table creation failed")
    spark.sql.side_effect = failure

    with (
        caplog.at_level(logging.INFO, logger=gold_stage.__name__),
        pytest.raises(RuntimeError, match="Gold table creation failed") as caught,
    ):
        gold_stage.table_asus_proart_p16_h7606wv(cast(SparkSession, spark), goldconfig)

    assert caught.value is failure
    spark.sql.assert_called_once()
    assert not [record for record in caplog.records if record.name == gold_stage.__name__]
