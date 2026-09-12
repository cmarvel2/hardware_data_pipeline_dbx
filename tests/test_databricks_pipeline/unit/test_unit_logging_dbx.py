from pathlib import Path
import logging.config

import pytest
import yaml

from databricks_pipeline.common import logging_dbx

pytestmark = pytest.mark.unit


@pytest.fixture
def conf_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    fake_module = (
        tmp_path
        / "databricks_pipeline"
        / "common"
        / "logging_dbx.py"
    )
    fake_module.parent.mkdir(parents=True)
    fake_module.touch()

    monkeypatch.setattr(logging_dbx, "__file__", str(fake_module))

    configuration_directory = fake_module.parents[1] / "conf"
    configuration_directory.mkdir()
    return configuration_directory


def write_yaml(
    configuration_directory: Path,
    filename: str,
    contents: dict[str, object],
) -> None:
    (configuration_directory / filename).write_text(
        yaml.safe_dump(contents),
        encoding="utf-8",
    )


def test_logging_setup_loads_yaml_and_applies_dict_config(
    conf_dir: Path,
    mocker,
) -> None:
    expected_config = {
        "version": 1,
        "disable_existing_loggers": False,
    }
    write_yaml(conf_dir, "logging_dbx.yml", expected_config)
    dict_config = mocker.patch.object(logging.config, "dictConfig")

    result = logging_dbx.logging_setup()

    assert result is None
    dict_config.assert_called_once_with(config=expected_config)


def test_logging_setup_accepts_path_filename(
    conf_dir: Path,
    mocker,
) -> None:
    expected_config = {"version": 1}
    write_yaml(conf_dir, "custom_logging.yml", expected_config)
    dict_config = mocker.patch.object(logging.config, "dictConfig")

    logging_dbx.logging_setup(Path("custom_logging.yml"))

    dict_config.assert_called_once_with(config=expected_config)


def test_logging_setup_raises_file_not_found_error_for_missing_config(
    conf_dir: Path,
) -> None:
    with pytest.raises(FileNotFoundError):
        logging_dbx.logging_setup("missing.yml")


def test_logging_setup_raises_yaml_error_for_malformed_config(
    conf_dir: Path,
) -> None:
    (conf_dir / "broken.yml").write_text(
        "handlers: [unclosed",
        encoding="utf-8",
    )

    with pytest.raises(yaml.YAMLError):
        logging_dbx.logging_setup("broken.yml")