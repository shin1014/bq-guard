from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from platformdirs import user_cache_dir


@dataclass
class TableMeta:
    partition_type: str
    partition_key: Optional[str]
    ingestion_time: bool
    last_seen_ts: str


class TableMetaCache:
    def __init__(self, schema_version: int) -> None:
        self.schema_version = schema_version
        self.tables: Dict[str, TableMeta] = {}
        self.path = Path(user_cache_dir("bq_guard")) / "table_meta_cache.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text())
        except Exception:
            return
        if data.get("schema_version") != self.schema_version:
            return
        tables = data.get("tables", {})
        for key, value in tables.items():
            self.tables[key] = TableMeta(
                partition_type=value.get("partition_type", "none"),
                partition_key=value.get("partition_key"),
                ingestion_time=bool(value.get("ingestion_time", False)),
                last_seen_ts=value.get("last_seen_ts", ""),
            )

    def save(self) -> None:
        data = {
            "schema_version": self.schema_version,
            "tables": {
                key: {
                    "partition_type": meta.partition_type,
                    "partition_key": meta.partition_key,
                    "ingestion_time": meta.ingestion_time,
                    "last_seen_ts": meta.last_seen_ts,
                }
                for key, meta in self.tables.items()
            },
        }
        self.path.write_text(json.dumps(data, ensure_ascii=False, indent=2))

    def get(self, table_key: str) -> Optional[TableMeta]:
        return self.tables.get(table_key)

    def update(self, table_key: str, meta: TableMeta) -> None:
        self.tables[table_key] = meta

    @staticmethod
    def now_ts() -> str:
        return datetime.now(timezone.utc).isoformat()
