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


def serving_dir(cfg: dict[str, Any], run: str) -> Path:
    """Folder for a stream run's outputs (positions, gap_events, minute_counts): live or replay."""
    return cfg["paths"]["serving"] if run == "live" else cfg["paths"]["replay_serving"]
