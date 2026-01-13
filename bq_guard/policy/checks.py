from __future__ import annotations

import re
from typing import List

from bq_guard.policy.sql_sanitize import sanitize_sql
from bq_guard.policy.types import Finding, Severity

SELECT_STAR = re.compile(r"\bselect\s+[^;]*?\*", re.IGNORECASE | re.DOTALL)
ALIAS_STAR = re.compile(r"\b\w+\.\*", re.IGNORECASE)
CROSS_JOIN = re.compile(r"\bcross\s+join\b", re.IGNORECASE)
JOIN = re.compile(r"\bjoin\b", re.IGNORECASE)
ON_OR_USING = re.compile(r"\b(on|using)\b", re.IGNORECASE)
STATEMENT_SPLIT = re.compile(r";\s*")
DDL_DML = {"insert", "update", "delete", "merge", "create", "drop", "alter", "truncate"}
SCRIPT_TOKENS = re.compile(r"\b(begin|end|declare)\b", re.IGNORECASE)


def check_bytes(bytes_processed: int | None, warn: int, block: int) -> List[Finding]:
    findings: List[Finding] = []
    if bytes_processed is None:
        return findings
    if bytes_processed >= block:
        findings.append(
            Finding(
                severity=Severity.ERROR,
                code="BYTES_BLOCK",
                message=f"推定処理量がブロック閾値を超過: {bytes_processed} bytes",
            )
        )
    elif bytes_processed >= warn:
        findings.append(
            Finding(
                severity=Severity.WARN,
                code="BYTES_WARN",
                message=f"推定処理量が警告閾値を超過: {bytes_processed} bytes",
            )
        )
    return findings


def check_select_star(sql: str) -> List[Finding]:
    sanitized = sanitize_sql(sql)
    if SELECT_STAR.search(sanitized) or ALIAS_STAR.search(sanitized):
        return [
            Finding(
                severity=Severity.WARN,
                code="SELECT_STAR",
                message="SELECT * を検知しました",
            )
        ]
    return []


def check_cross_join(sql: str) -> List[Finding]:
    if CROSS_JOIN.search(sanitize_sql(sql)):
        return [
            Finding(
                severity=Severity.WARN,
                code="CROSS_JOIN",
                message="CROSS JOIN を検知しました",
            )
        ]
    return []


def check_suspect_join(sql: str) -> List[Finding]:
    sanitized = sanitize_sql(sql)
    if JOIN.search(sanitized) and not ON_OR_USING.search(sanitized):
        return [
            Finding(
                severity=Severity.WARN,
                code="SUSPECT_JOIN",
                message="JOIN 条件が見当たりません (ON/USING 未検知)",
            )
        ]
    return []


def check_multi_statement(sql: str, block: bool) -> List[Finding]:
    sanitized = sanitize_sql(sql).strip()
    if SCRIPT_TOKENS.search(sanitized):
        return [
            Finding(
                severity=Severity.ERROR if block else Severity.WARN,
                code="SCRIPT",
                message="スクリプト構文を検知しました",
            )
        ]
    statements = [s for s in STATEMENT_SPLIT.split(sanitized) if s.strip()]
    if len(statements) > 1:
        return [
            Finding(
                severity=Severity.ERROR if block else Severity.WARN,
                code="MULTI_STATEMENT",
                message="複数ステートメントを検知しました",
            )
        ]
    return []


def check_ddl_dml(sql: str) -> List[Finding]:
    sanitized = sanitize_sql(sql).strip()
    match = re.match(r"^(\w+)", sanitized, re.IGNORECASE)
    if match and match.group(1).lower() in DDL_DML:
        return [
            Finding(
                severity=Severity.WARN,
                code="DDL_DML",
                message=f"DDL/DML を検知しました: {match.group(1).upper()}",
            )
        ]
    return []


def run_policy_checks(sql: str, config: dict, bytes_processed: int | None) -> List[Finding]:
    findings: List[Finding] = []
    findings.extend(check_bytes(bytes_processed, config["limits"]["warn_bytes"], config["limits"]["block_bytes"]))
    if config["policy"].get("warn_select_star"):
        findings.extend(check_select_star(sql))
    if config["policy"].get("warn_cross_join"):
        findings.extend(check_cross_join(sql))
    if config["policy"].get("warn_suspect_join"):
        findings.extend(check_suspect_join(sql))
    if config["policy"].get("block_multi_statement"):
        findings.extend(check_multi_statement(sql, True))
    else:
        findings.extend(check_multi_statement(sql, False))
    if config["policy"].get("warn_ddl_dml"):
        findings.extend(check_ddl_dml(sql))
    return findings
