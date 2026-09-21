from pathlib import Path
from typing import cast

import pytest
import yaml
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

AUTH_PROFILE = "test"
BUNDLE_TARGET = "test"


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
