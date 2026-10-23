"""Load the project configuration from config.yaml."""

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config.yaml"


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Return the config dict with every entry under `paths` resolved to an absolute Path."""
    with open(path) as f:
        cfg = yaml.safe_load(f)
    cfg["paths"] = {name: ROOT / p for name, p in cfg["paths"].items()}
    return cfg
