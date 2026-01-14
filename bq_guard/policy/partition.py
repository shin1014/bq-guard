from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from bq_guard.cache import TableMeta
from bq_guard.policy.types import Finding, Severity


def _has_partition_filter(sql: str, key: str) -> bool:
    return re.search(rf"\b{re.escape(key)}\b", sql, re.IGNORECASE) is not None


def enforce_partition_filters(
    sql: str,
    referenced_tables: List[str],
    table_meta: Dict[str, TableMeta],
    config: dict,
) -> Tuple[List[Finding], List[str]]:
    findings: List[Finding] = []
    summary: List[str] = []
    if not referenced_tables:
        findings.append(
            Finding(
                severity=Severity.WARN,
                code="PARTITION_TABLES_UNKNOWN",
                message="参照テーブルを特定できませんでした",
            )
        )
        return findings, summary

    exempt = set(config.get("exceptions", {}).get("partition_exempt_tables", []))
    for table in referenced_tables:
        if table in exempt:
            summary.append(f"{table}: exempt")
            continue
        meta = table_meta.get(table)
        if not meta or meta.partition_type == "none":
            summary.append(f"{table}: non-partition")
            continue
        if meta.ingestion_time:
            ok = _has_partition_filter(sql, "_PARTITIONDATE") or _has_partition_filter(
                sql, "_PARTITIONTIME"
            )
            if not ok:
                findings.append(
                    Finding(
                        severity=Severity.ERROR,
                        code="PARTITION_MISSING",
                        message="パーティションフィルタが必要です",
                        evidence="_PARTITIONDATE/_PARTITIONTIME",
                        table=table,
                    )
                )
                summary.append(f"{table}: missing _PARTITIONDATE/_PARTITIONTIME")
            else:
                summary.append(f"{table}: ok")
            continue
        if meta.partition_key:
            if not _has_partition_filter(sql, meta.partition_key):
                findings.append(
                    Finding(
                        severity=Severity.ERROR,
                        code="PARTITION_MISSING",
                        message="パーティションフィルタが必要です",
                        evidence=meta.partition_key,
                        table=table,
                    )
                )
                summary.append(f"{table}: missing {meta.partition_key}")
            else:
                summary.append(f"{table}: ok")
    return findings, summary
