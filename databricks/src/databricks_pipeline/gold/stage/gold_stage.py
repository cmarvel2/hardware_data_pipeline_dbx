import logging

from pyspark.sql import SparkSession

from databricks_pipeline.gold.config.gold_config import GoldConfig

logger = logging.getLogger(__name__)


def table_asus_proart_p16_h7606wv(spark: SparkSession, config: GoldConfig) -> None:
    spark.sql(f"""   
        CREATE OR REPLACE TABLE
        {config.target_table("mart_proart_p16_h7606wv")} AS
        SELECT s.test_id,
        ht.tested_at,
        ht.device_id,
        ht.testing_software,
        s.hardware_name, 
        s.sensor_name,
        s.sensor_type,
        s.sensor_value,
        s.polled_at,
        CASE
            WHEN s.polled_at < Min(s.polled_at) OVER (PARTITION BY s.test_id) + INTERVAL '10' MINUTES THEN TRUE
            ELSE FALSE
        END AS thermal_rampup_phase   
        FROM {config.source_table("fct_sensors")} s
        JOIN {config.source_table("dim_hardware_tests")} ht ON s.test_id = ht.test_id
        WHERE ht.testing_software:device::STRING = 'ASUS_ProArt_P16_(H7606)'
            """)
    logger.info("Materialized view hardware_dev.gold.mart_proart_p16_h7606wv created/updated")
