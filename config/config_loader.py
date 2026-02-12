import yaml
from pathlib import Path

_CONFIG_CACHE = {}


def load_config(path: str) -> dict:
    """
    Load YAML config with per-file cache.
    """

    if path in _CONFIG_CACHE:
        return _CONFIG_CACHE[path]

    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    _CONFIG_CACHE[path] = data
    return data


