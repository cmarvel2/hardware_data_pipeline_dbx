import logging.config
from pathlib import Path

import yaml


def logging_setup(filename: str | Path = "logging_dbx.yml") -> None:
    dab_root = Path(__file__).resolve().parents[1]
    targetpath = dab_root / "conf" / filename

    with open(targetpath) as ofile:
        logging_config = yaml.safe_load(ofile)

    logging.config.dictConfig(config=logging_config)
