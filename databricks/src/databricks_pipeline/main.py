import argparse

from pyspark.sql import SparkSession

from databricks_pipeline.bronze.config import bronze_config
from databricks_pipeline.bronze.stage.bronze_stage import landing_hardware_autoloader
from databricks_pipeline.common import logging_dbx


def main():
    logging_dbx.logging_setup()

    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--landing-volume", required=True)
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Hardware Data Pipeline").getOrCreate()

    if args.stage == "bronze":
        bcfg = bronze_config.BronzeConfig(
            catalog=args.catalog,
            bronze_schema=args.bronze_schema,
            landing_volume=args.landing_volume,
            bronze_table=args.bronze_table,
        )

        landing_hardware_autoloader(spark=spark, config=bcfg, run_id=args.pipeline_run_id)
    else:
        parser.error(f"{args.stage} is not a selectable stage")


if __name__ == "__main__":
    main()
