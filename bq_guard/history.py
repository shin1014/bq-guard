from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from platformdirs import user_state_dir


def history_path() -> Path:
    path = Path(user_state_dir("bq_guard")) / "history.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def append_history(record: Dict[str, Any]) -> None:
    record = dict(record)
    record.setdefault("ts", datetime.now(timezone.utc).isoformat())
    path = history_path()
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
