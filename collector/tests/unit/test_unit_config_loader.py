from collections.abc import Mapping
from pathlib import Path

import pytest
import yaml

from local_data_collection.utils import config_loader

pytestmark = pytest.mark.unit


@pytest.fixture
def conf_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    fake_module = tmp_path / "local_data_collection" / "utils" / "config_loader.py"
    fake_module.parent.mkdir(parents=True)
    fake_module.touch()

    monkeypatch.setattr(config_loader, "__file__", str(fake_module))

    configuration_directory = fake_module.parents[1] / "conf"
    configuration_directory.mkdir()
    return configuration_directory


def write_yaml(
    configuration_directory: Path,
    filename: str,
    contents: Mapping[str, object],
) -> None:
    (configuration_directory / filename).write_text(
        yaml.safe_dump(contents),
        encoding="utf-8",
    )


def test_load_conf_returns_parsed_yaml_mapping(conf_dir: Path) -> None:
    expected = {"occt": {"stability_test": {"length_in_minutes": 5}}}
    write_yaml(conf_dir, "sensors.yml", expected)

    assert config_loader.load_conf("sensors.yml") == expected


def test_load_conf_accepts_path_filename(conf_dir: Path) -> None:
    expected = {"key": "value"}
    write_yaml(conf_dir, "settings.yml", expected)

    assert config_loader.load_conf(Path("settings.yml")) == expected


def test_load_conf_raises_file_not_found_error_for_missing_file(conf_dir: Path) -> None:
    with pytest.raises(FileNotFoundError):
        config_loader.load_conf("missing.yml")


def test_load_conf_raises_yaml_error_for_malformed_file(conf_dir: Path) -> None:
    (conf_dir / "broken.yml").write_text("key: [unclosed", encoding="utf-8")

    with pytest.raises(yaml.YAMLError):
        config_loader.load_conf("broken.yml")


def test_load_conf_returns_none_for_empty_yaml_file(conf_dir: Path) -> None:
    (conf_dir / "empty.yml").write_text("", encoding="utf-8")

    assert config_loader.load_conf("empty.yml") is None
