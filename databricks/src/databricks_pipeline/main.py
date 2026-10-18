import argparse

from pyspark.sql import SparkSession

from databricks_pipeline.bronze.config import bronze_config
from databricks_pipeline.bronze.stage.bronze_stage import landing_hardware_autoloader
from databricks_pipeline.common import logging_dbx, ops_config
from databricks_pipeline.silver.config import silver_config
from databricks_pipeline.silver.stage.silver_stage import (
    bronze_data_cleaning,
    normalized_bronze_tables,
    write_incoming_quarantine,
    write_outgoing_quarantine,
    write_to_silver,
)


def main():
    logging_dbx.logging_setup()

    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="stage", required=True)
    bronze_parser = commands.add_parser("bronze")
    silver_parser = commands.add_parser("silver")

    bronze_parser.add_argument("--catalog", required=True)
    bronze_parser.add_argument("--bronze-schema", required=True)
    bronze_parser.add_argument("--landing-volume", required=True)
    bronze_parser.add_argument("--bronze-table", required=True)
    bronze_parser.add_argument("--ops-schema", required=True)
    bronze_parser.add_argument("--ops-volume", required=True)
    bronze_parser.add_argument("--pipeline-run-id", required=True)

    silver_parser.add_argument("--catalog", required=True)
    silver_parser.add_argument("--bronze-schema", required=True)
    silver_parser.add_argument("--bronze-table", required=True)
    silver_parser.add_argument("--silver-schema", required=True)
    silver_parser.add_argument("--silver-sensors-table", required=True)
    silver_parser.add_argument("--silver-dbx-metadata-table", required=True)
    silver_parser.add_argument("--silver-tests-table", required=True)
    silver_parser.add_argument("--ops-schema", required=True)
    silver_parser.add_argument("--ops-volume", required=True)
    silver_parser.add_argument("--inc-quarantine-table", required=True)
    silver_parser.add_argument("--out-quarantine-table", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Hardware Data Pipeline").getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    if args.stage == "bronze":
        bcfg = bronze_config.BronzeConfig(
            catalog=args.catalog,
            bronze_schema=args.bronze_schema,
            landing_volume=args.landing_volume,
            bronze_table=args.bronze_table,
        )

        ccfg = ops_config.CommonConfig(
            catalog=args.catalog,
            ops_volume=args.ops_volume,
            ops_schema=args.ops_schema,
            incoming_quarantine_table=None,
            outgoing_quarantine_table=None,
        )

        landing_hardware_autoloader(spark=spark, config=bcfg, commonconfig=ccfg, run_id=args.pipeline_run_id)
    elif args.stage == "silver":
        scfg = silver_config.SilverConfig(
            catalog=args.catalog,
            bronze_schema=args.bronze_schema,
            bronze_table=args.bronze_table,
            silver_schema=args.silver_schema,
        )

        ccfg = ops_config.CommonConfig(
            catalog=args.catalog,
            ops_volume=args.ops_volume,
            ops_schema=args.ops_schema,
            incoming_quarantine_table=args.inc_quarantine_table,
            outgoing_quarantine_table=args.out_quarantine_table,
        )

        valid_bronze_df, inc_quarantine_df, out_quarantine_df = bronze_data_cleaning(spark=spark, config=scfg)

        fct_sensors, dim_tests, dim_dbx_metadata = normalized_bronze_tables(cleaned_bronze_df=valid_bronze_df)

        write_incoming_quarantine(inc_quarantine_df, commonconfig=ccfg)
        write_outgoing_quarantine(out_quarantine_df, commonconfig=ccfg)

        write_to_silver(silver_df=fct_sensors, config=scfg, commonconfig=ccfg, tablename=args.silver_sensors_table)
        write_to_silver(silver_df=dim_tests, config=scfg, commonconfig=ccfg, tablename=args.silver_tests_table)
        write_to_silver(
            silver_df=dim_dbx_metadata, config=scfg, commonconfig=ccfg, tablename=args.silver_dbx_metadata_table
        )
    else:
        parser.error(f"{args.stage} is not a selectable stage")


if __name__ == "__main__":
    main()
