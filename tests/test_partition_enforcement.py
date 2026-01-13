from bq_guard.cache import TableMeta
from bq_guard.policy.partition import enforce_partition_filters
from bq_guard.policy.types import Severity


def test_ingestion_time_partition_ok():
    meta = TableMeta(
        partition_type="time",
        partition_key="_PARTITIONDATE",
        ingestion_time=True,
        last_seen_ts="",
    )
    findings, summary = enforce_partition_filters(
        "SELECT * FROM t WHERE _PARTITIONDATE = '2024-01-01'",
        ["p.d.t"],
        {"p.d.t": meta},
        {"exceptions": {"partition_exempt_tables": []}},
    )
    assert not findings
    assert "ok" in "".join(summary)


def test_ingestion_time_partition_missing():
    meta = TableMeta(
        partition_type="time",
        partition_key="_PARTITIONDATE",
        ingestion_time=True,
        last_seen_ts="",
    )
    findings, _ = enforce_partition_filters(
        "SELECT * FROM t",
        ["p.d.t"],
        {"p.d.t": meta},
        {"exceptions": {"partition_exempt_tables": []}},
    )
    assert findings
    assert findings[0].severity == Severity.ERROR


def test_column_partition_missing():
    meta = TableMeta(
        partition_type="time",
        partition_key="event_date",
        ingestion_time=False,
        last_seen_ts="",
    )
    findings, _ = enforce_partition_filters(
        "SELECT * FROM t",
        ["p.d.t"],
        {"p.d.t": meta},
        {"exceptions": {"partition_exempt_tables": []}},
    )
    assert findings
    assert findings[0].code == "PARTITION_MISSING"


def test_exempt_tables_skip():
    meta = TableMeta(
        partition_type="time",
        partition_key="event_date",
        ingestion_time=False,
        last_seen_ts="",
    )
    findings, _ = enforce_partition_filters(
        "SELECT * FROM t",
        ["p.d.t"],
        {"p.d.t": meta},
        {"exceptions": {"partition_exempt_tables": ["p.d.t"]}},
    )
    assert not findings


def test_unknown_tables_warn():
    findings, _ = enforce_partition_filters(
        "SELECT * FROM t",
        [],
        {},
        {"exceptions": {"partition_exempt_tables": []}},
    )
    assert findings
    assert findings[0].severity == Severity.WARN
