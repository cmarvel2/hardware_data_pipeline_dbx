import sys
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import Mock, call

import pytest
import yaml
from pytest_mock import MockerFixture

from databricks_pipeline import main as entrypoint

pytestmark = pytest.mark.unit


def stage_arguments(stage: str) -> list[str]:
    shared = [
        "--catalog",
        "hardware_test",
        "--bronze-schema",
        "bronze",
        "--bronze-table",
        "sensor_readings",
        "--ops-schema",
        "ops",
        "--ops-volume",
        "pipeline_operations",
    ]
    specific = (
        ["--landing-volume", "hardware_landing", "--pipeline-run-id", "run-123"]
        if stage == "bronze"
        else [
            "--silver-schema",
            "silver",
            "--silver-sensors-table",
            "fct_sensors",
            "--silver-tests-table",
            "dim_hardware_tests",
            "--silver-dbx-metadata-table",
            "dim_dbx_metadata",
            "--inc-quarantine-table",
            "incoming_silver_quarantine",
            "--out-quarantine-table",
            "outgoing_silver_quarantine",
        ]
    )
    return ["main", stage, *shared, *specific]


@dataclass(frozen=True)
class EntrypointSetup:
    session_class: Mock
    spark: Mock
    bronze: Mock
    cleaning: Mock
    normalize: Mock
    incoming: Mock
    outgoing: Mock
    silver: Mock


@pytest.fixture
def entrypoint_setup(mocker: MockerFixture) -> EntrypointSetup:
    mocker.patch.object(entrypoint.logging_dbx, "logging_setup")
    session_class = mocker.patch.object(entrypoint, "SparkSession")
    spark = session_class.builder.appName.return_value.getOrCreate.return_value
    names = [
        "landing_hardware_autoloader",
        "bronze_data_cleaning",
        "normalized_bronze_tables",
        "write_incoming_quarantine",
        "write_outgoing_quarantine",
        "write_to_silver",
    ]
    patched = [mocker.patch.object(entrypoint, name) for name in names]
    bronze, cleaning, normalize, incoming, outgoing, silver = patched
    cleaning.return_value = (Mock(name="valid"), Mock(name="incoming"), Mock(name="outgoing"))
    normalize.return_value = (Mock(name="facts"), Mock(name="tests"), Mock(name="files"))
    return EntrypointSetup(session_class, spark, bronze, cleaning, normalize, incoming, outgoing, silver)


def test_bronze_subcommand_requires_no_silver_options(
    monkeypatch: pytest.MonkeyPatch,
    entrypoint_setup: EntrypointSetup,
) -> None:
    monkeypatch.setattr(sys, "argv", stage_arguments("bronze"))
    entrypoint.main()
    kwargs = entrypoint_setup.bronze.call_args.kwargs
    assert kwargs["spark"] is entrypoint_setup.spark
    assert kwargs["run_id"] == "run-123"
    assert kwargs["config"].target_table == "hardware_test.bronze.sensor_readings"
    assert kwargs["commonconfig"].ops_volume_path == "/Volumes/hardware_test/ops/pipeline_operations"
    entrypoint_setup.cleaning.assert_not_called()
    assert entrypoint_setup.spark.conf.set.call_args_list == [
        call("spark.sql.session.timeZone", "UTC"),
        call("spark.sql.timestampType", "TIMESTAMP_LTZ"),
    ]


def test_silver_subcommand_dispatches_all_outputs_without_landing_argument(
    monkeypatch: pytest.MonkeyPatch,
    entrypoint_setup: EntrypointSetup,
) -> None:
    monkeypatch.setattr(sys, "argv", stage_arguments("silver"))
    entrypoint.main()
    valid, incoming, outgoing = entrypoint_setup.cleaning.return_value
    entrypoint_setup.normalize.assert_called_once_with(cleaned_bronze_df=valid)
    entrypoint_setup.incoming.assert_called_once()
    entrypoint_setup.outgoing.assert_called_once()
    assert entrypoint_setup.incoming.call_args.args == (incoming,)
    assert entrypoint_setup.outgoing.call_args.args == (outgoing,)
    assert [item.kwargs["tablename"] for item in entrypoint_setup.silver.call_args_list] == [
        "fct_sensors",
        "dim_hardware_tests",
        "dim_dbx_metadata",
    ]
    assert [item.kwargs["silver_df"] for item in entrypoint_setup.silver.call_args_list] == list(
        entrypoint_setup.normalize.return_value
    )
    entrypoint_setup.bronze.assert_not_called()


@pytest.mark.parametrize(
    ("stage", "missing"),
    [("bronze", "--landing-volume"), ("bronze", "--pipeline-run-id"), ("silver", "--silver-tests-table")],
)
def test_missing_stage_arguments_fail_before_spark_starts(
    monkeypatch: pytest.MonkeyPatch,
    entrypoint_setup: EntrypointSetup,
    capsys: pytest.CaptureFixture[str],
    stage: str,
    missing: str,
) -> None:
    arguments = stage_arguments(stage)
    index = arguments.index(missing)
    del arguments[index : index + 2]
    monkeypatch.setattr(sys, "argv", arguments)
    with pytest.raises(SystemExit) as caught:
        entrypoint.main()
    assert caught.value.code == 2
    assert missing in capsys.readouterr().err
    entrypoint_setup.session_class.builder.appName.assert_not_called()


@pytest.mark.parametrize("stage", ["bronze", "silver"])
def test_stage_help_does_not_start_spark(
    monkeypatch: pytest.MonkeyPatch,
    entrypoint_setup: EntrypointSetup,
    capsys: pytest.CaptureFixture[str],
    stage: str,
) -> None:
    monkeypatch.setattr(sys, "argv", ["main", stage, "--help"])
    with pytest.raises(SystemExit) as caught:
        entrypoint.main()
    assert caught.value.code == 0
    help_text = capsys.readouterr().out
    assert ("--landing-volume" in help_text) == (stage == "bronze")
    assert ("--silver-schema" in help_text) == (stage == "silver")
    entrypoint_setup.session_class.builder.appName.assert_not_called()


@pytest.mark.parametrize("task_key", ["bronze_ingest", "silver_transformation"])
def test_job_task_arguments_are_accepted_by_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
    entrypoint_setup: EntrypointSetup,
    task_key: str,
) -> None:
    job_path = Path(__file__).resolve().parents[2] / "resources" / "hardware_pipeline_job.yml"
    job = yaml.safe_load(job_path.read_text(encoding="utf-8"))["resources"]["jobs"]["hardware_pipeline_job"]
    task = next(task for task in job["tasks"] if task["task_key"] == task_key)
    monkeypatch.setattr(sys, "argv", ["main", *task["python_wheel_task"]["parameters"]])
    entrypoint.main()
    if task_key == "bronze_ingest":
        entrypoint_setup.bronze.assert_called_once()
    else:
        entrypoint_setup.cleaning.assert_called_once()
