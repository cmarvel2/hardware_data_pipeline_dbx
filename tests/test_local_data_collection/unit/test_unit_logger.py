
import logging.config

import pytest

from local_data_collection.utils import logging_ldc

pytestmark = pytest.mark.unit


def test_logging_setup_loads_named_configuration_and_applies_it(mocker) -> None:
    expected_configuration = {
        "version": 1,
        "disable_existing_loggers": False,
    }
    load_conf = mocker.patch.object(
        logging_ldc.config_loader,
        "load_conf",
        return_value=expected_configuration,
    )
    dict_config = mocker.patch.object(logging.config, "dictConfig")

    logging_ldc.logging_setup("logging_ldc.yml")

    load_conf.assert_called_once_with("logging_ldc.yml")
    dict_config.assert_called_once_with(config=expected_configuration)


def test_logging_setup_defaults_to_logging_ldc_yml(mocker) -> None:
    load_conf = mocker.patch.object(
        logging_ldc.config_loader,
        "load_conf",
        return_value={"version": 1},
    )
    mocker.patch.object(logging.config, "dictConfig")

    logging_ldc.logging_setup()

    load_conf.assert_called_once_with("logging_ldc.yml")


def test_logging_setup_propagates_missing_configuration_error(mocker) -> None:
    mocker.patch.object(
        logging_ldc.config_loader,
        "load_conf",
        side_effect=FileNotFoundError("missing.yml"),
    )
    dict_config = mocker.patch.object(logging.config, "dictConfig")

    with pytest.raises(FileNotFoundError, match="missing.yml"):
        logging_ldc.logging_setup("missing.yml")

    dict_config.assert_not_called()