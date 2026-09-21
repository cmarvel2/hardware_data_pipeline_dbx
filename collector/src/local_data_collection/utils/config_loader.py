from pathlib import Path

import yaml


def load_conf(filename: str | Path) -> dict:
    collector_root = Path(__file__).resolve().parents[1]
    targetpath = collector_root / "conf" / filename

    with open(targetpath) as ofile:
        return yaml.safe_load(ofile)
