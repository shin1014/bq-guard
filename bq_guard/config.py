from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml
from platformdirs import user_config_dir


DEFAULT_CONFIG: Dict[str, Any] = {
    "app": {
        "default_project": None,
        "default_location": None,
        "preview_rows": 50,
        "page_size": 1000,
    },
    "limits": {
        "warn_bytes": 107_374_182_400,
        "block_bytes": 536_870_912_000,
    },
    "policy": {
        "enforce_partition_filter": True,
        "block_multi_statement": True,
        "warn_select_star": True,
        "warn_cross_join": True,
        "warn_suspect_join": True,
        "warn_ddl_dml": True,
        "allow_execute_with_warnings": True,
    },
    "exceptions": {
        "partition_exempt_tables": [],
    },
    "cache": {
        "schema_version": 1,
    },
    "bq": {
        "use_query_cache": False,
        "labels": {"app": "bq-guard", "env": "gce"},
    },
    "ui": {
        "auto_estimate_debounce_ms": 900,
        "show_left_settings_panel": False,
    },
}


@dataclass
class ConfigResult:
    config: Dict[str, Any]
    warnings: List[str]
    path: Path


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _validate_config(config: Dict[str, Any]) -> List[str]:
    warnings: List[str] = []

    def ensure_int(path: Tuple[str, ...], minimum: int | None = None) -> None:
        current = config
        for key in path[:-1]:
            current = current.get(key, {})
        key = path[-1]
        value = current.get(key)
        if not isinstance(value, int):
            warnings.append(f"Invalid type for {'.'.join(path)}; using default.")
            default_value = DEFAULT_CONFIG
            for part in path:
                default_value = default_value[part]
            current[key] = default_value
            return
        if minimum is not None and value < minimum:
            warnings.append(f"Invalid value for {'.'.join(path)}; using default.")
            default_value = DEFAULT_CONFIG
            for part in path:
                default_value = default_value[part]
            current[key] = default_value

    def ensure_bool(path: Tuple[str, ...]) -> None:
        current = config
        for key in path[:-1]:
            current = current.get(key, {})
        key = path[-1]
        value = current.get(key)
        if not isinstance(value, bool):
            warnings.append(f"Invalid type for {'.'.join(path)}; using default.")
            default_value = DEFAULT_CONFIG
            for part in path:
                default_value = default_value[part]
            current[key] = default_value

    ensure_int(("app", "preview_rows"), minimum=1)
    ensure_int(("app", "page_size"), minimum=1)
    ensure_int(("limits", "warn_bytes"), minimum=0)
    ensure_int(("limits", "block_bytes"), minimum=0)
    ensure_int(("cache", "schema_version"), minimum=1)

    ensure_bool(("policy", "enforce_partition_filter"))
    ensure_bool(("policy", "block_multi_statement"))
    ensure_bool(("policy", "warn_select_star"))
    ensure_bool(("policy", "warn_cross_join"))
    ensure_bool(("policy", "warn_suspect_join"))
    ensure_bool(("policy", "warn_ddl_dml"))
    ensure_bool(("policy", "allow_execute_with_warnings"))

    debounce = config.get("ui", {}).get("auto_estimate_debounce_ms")
    if not isinstance(debounce, int) or debounce < 0:
        warnings.append("Invalid ui.auto_estimate_debounce_ms; using default.")
        config["ui"]["auto_estimate_debounce_ms"] = DEFAULT_CONFIG["ui"][
            "auto_estimate_debounce_ms"
        ]

    preview_rows = config.get("app", {}).get("preview_rows")
    if not isinstance(preview_rows, int) or preview_rows < 1:
        warnings.append("Invalid app.preview_rows; using default.")
        config["app"]["preview_rows"] = DEFAULT_CONFIG["app"]["preview_rows"]

    page_size = config.get("app", {}).get("page_size")
    if not isinstance(page_size, int) or page_size < 1:
        warnings.append("Invalid app.page_size; using default.")
        config["app"]["page_size"] = DEFAULT_CONFIG["app"]["page_size"]

    if not isinstance(config.get("exceptions", {}).get("partition_exempt_tables"), list):
        warnings.append("Invalid exceptions.partition_exempt_tables; using default.")
        config["exceptions"]["partition_exempt_tables"] = []

    if not isinstance(config.get("bq", {}).get("labels"), dict):
        warnings.append("Invalid bq.labels; using default.")
        config["bq"]["labels"] = DEFAULT_CONFIG["bq"]["labels"]

    return warnings


def load_config() -> ConfigResult:
    config_dir = Path(user_config_dir("bq_guard"))
    config_dir.mkdir(parents=True, exist_ok=True)
    path = config_dir / "config.yaml"
    raw: Dict[str, Any] = {}
    if path.exists():
        try:
            raw = yaml.safe_load(path.read_text()) or {}
        except Exception:
            raw = {}
    merged = _deep_merge(DEFAULT_CONFIG, raw)
    warnings = _validate_config(merged)
    if not path.exists():
        path.write_text(yaml.safe_dump(merged, sort_keys=False))
    return ConfigResult(config=merged, warnings=warnings, path=path)


def save_config(config: Dict[str, Any]) -> None:
    path = Path(user_config_dir("bq_guard")) / "config.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False))
