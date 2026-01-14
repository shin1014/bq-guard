from __future__ import annotations

import subprocess
from typing import Optional


def _get_value(path: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", path],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    if not value or value == "(unset)":
        return None
    return value


def get_default_project() -> Optional[str]:
    return _get_value("project")


def get_default_location() -> Optional[str]:
    for key in ("dataproc/region", "run/region", "compute/region"):
        value = _get_value(key)
        if value:
            return value
    return None
