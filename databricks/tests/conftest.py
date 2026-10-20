import os
from collections.abc import Iterator
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
import yaml
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from databricks_pipeline.common.ops_config import CommonConfig
from databricks_pipeline.gold.config.gold_config import GoldConfig

AUTH_PROFILE = "test"
BUNDLE_TARGET = "test"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "e2e: complete Databricks landing-to-Gold pipeline test")


@pytest.fixture
def lakehouse(
    request: pytest.FixtureRequest, test_target: dict[str, object]
) -> Iterator[tuple[SparkSession, GoldConfig, str]]:
    if os.getenv("RUN_DATABRICKS_TESTS") != "1":
        pytest.skip("Set RUN_DATABRICKS_TESTS=1 to create isolated test schemas and run cloud tests.")

    session = cast(SparkSession, request.getfixturevalue("spark"))
    variables = cast(dict[str, str], test_target["variables"])
    catalog = variables["catalog"]
    prefix = f"pytest_{uuid4().hex}"
    bronze_schema, silver_schema, gold_schema = (f"{prefix}_{layer}" for layer in ("bronze", "silver", "gold"))
    created: list[str] = []
    try:
        for schema in (bronze_schema, silver_schema, gold_schema):
            qualified = f"{catalog}.{schema}"
            session.sql(f"CREATE SCHEMA {qualified}")
            created.append(qualified)
        yield session, GoldConfig(catalog, silver_schema, gold_schema), bronze_schema
    finally:
        for qualified in reversed(created):
            session.sql(f"DROP SCHEMA IF EXISTS {qualified} CASCADE")


@pytest.fixture
def commonconfig() -> CommonConfig:
    return CommonConfig(
        catalog="hardware_test",
        ops_volume="pipeline_operations",
        ops_schema="ops",
        incoming_quarantine_table="incoming_silver_quarantine",
        outgoing_quarantine_table="outgoing_silver_quarantine",
    )


@pytest.fixture(scope="session")
def test_target() -> dict[str, object]:
    bundle_path = Path(__file__).resolve().parents[1] / "databricks.yml"

    with bundle_path.open(encoding="utf-8") as bundle_file:
        bundle_config = cast(
            dict[str, object],
            yaml.safe_load(bundle_file),
        )

    targets = cast(
        dict[str, dict[str, object]],
        bundle_config["targets"],
    )

    return targets[BUNDLE_TARGET]


@pytest.fixture(scope="session")
def spark(test_target: dict[str, object]) -> SparkSession:
    workspace = cast(
        dict[str, str],
        test_target["workspace"],
    )
    expected_host = workspace["host"].rstrip("/")

    workspace_client = WorkspaceClient(profile=AUTH_PROFILE)
    actual_host = workspace_client.config.host

    if actual_host is None:
        pytest.fail(f"The Databricks auth profile '{AUTH_PROFILE}' does not define a host.")

    actual_host = actual_host.rstrip("/")

    if actual_host != expected_host:
        pytest.fail(
            f"The Databricks auth profile '{AUTH_PROFILE}' does not point to "
            f"the workspace declared by targets.{BUNDLE_TARGET} in "
            "databricks.yml."
        )

    return DatabricksSession.builder.serverless().profile(AUTH_PROFILE).getOrCreate()
