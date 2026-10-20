import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType

from databricks_pipeline.gold.config.gold_config import GoldConfig
from databricks_pipeline.gold.stage.gold_stage import table_asus_proart_p16_h7606wv

pytestmark = [pytest.mark.integration, pytest.mark.databricks]


def test_gold_preserves_reading_grain_and_per_test_rampup_boundaries(
    lakehouse: tuple[SparkSession, GoldConfig, str],
) -> None:
    spark, config, _ = lakehouse
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"""
        CREATE TABLE {config.source_table("fct_sensors")} USING DELTA AS
        SELECT test_id, 'CPU' AS hardware_name, 'CPU Power' AS sensor_name,
               'Power' AS sensor_type, CAST(40 AS DOUBLE) AS sensor_value,
               CAST(polled_at AS TIMESTAMP) AS polled_at
        FROM VALUES
            ('a', '2026-10-19 12:00:00.000'),
            ('a', '2026-10-19 12:09:59.999'),
            ('a', '2026-10-19 12:10:00.000'),
            ('a', '2026-10-19 12:10:00.001'),
            ('b', '2026-10-19 13:00:00.000'),
            ('b', '2026-10-19 13:10:00.000'),
            ('other', '2026-10-19 12:00:00.000') AS readings(test_id, polled_at)
    """)
    spark.sql(f"""
        CREATE TABLE {config.source_table("dim_hardware_tests")} USING DELTA AS
        SELECT test_id, TIMESTAMP '2026-10-19 12:00:00' AS tested_at,
               'test-device' AS device_id,
               to_variant_object(named_struct('device', device)) AS testing_software
        FROM VALUES
            ('a', 'ASUS_ProArt_P16_(H7606)'),
            ('b', 'ASUS_ProArt_P16_(H7606)'),
            ('other', 'different-device') AS tests(test_id, device)
    """)
    table_asus_proart_p16_h7606wv(spark, config)
    target = config.target_table("mart_proart_p16_h7606wv")
    result = spark.table(target)

    assert result.columns == [
        "test_id",
        "tested_at",
        "device_id",
        "testing_software",
        "hardware_name",
        "sensor_name",
        "sensor_type",
        "sensor_value",
        "polled_at",
        "thermal_rampup_phase",
    ]
    assert isinstance(result.schema["thermal_rampup_phase"].dataType, BooleanType)
    actual = {
        (row["test_id"], row["at"]): row["thermal_rampup_phase"]
        for row in result.selectExpr(
            "test_id", "date_format(polled_at, 'HH:mm:ss.SSS') AS at", "thermal_rampup_phase"
        ).collect()
    }
    assert actual == {
        ("a", "12:00:00.000"): True,
        ("a", "12:09:59.999"): True,
        ("a", "12:10:00.000"): False,
        ("a", "12:10:00.001"): False,
        ("b", "13:00:00.000"): True,
        ("b", "13:10:00.000"): False,
    }
    assert result.count() == 6

    table_asus_proart_p16_h7606wv(spark, config)
    assert spark.table(target).count() == 6
