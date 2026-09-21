import logging.config

from local_data_collection.utils import config_loader


def logging_setup(loggingfile: str = "logging_ldc.yml") -> None:
    logging_config = config_loader.load_conf(loggingfile)
    logging.config.dictConfig(config=logging_config)
