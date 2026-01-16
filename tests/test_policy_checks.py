from bq_guard.policy.checks import (
    check_bytes,
    check_cross_join,
    check_ddl_dml,
    check_multi_statement,
    check_select_star,
)
from bq_guard.policy.types import Severity


def test_select_star_detected():
    findings = check_select_star("SELECT * FROM foo")
    assert findings
    assert findings[0].code == "SELECT_STAR"


def test_cross_join_detected():
    findings = check_cross_join("SELECT 1 FROM a CROSS JOIN b")
    assert findings
    assert findings[0].code == "CROSS_JOIN"


def test_multi_statement_detection():
    findings = check_multi_statement("SELECT 1; SELECT 2", block=True)
    assert findings
    assert findings[0].code == "MULTI_STATEMENT"
    findings = check_multi_statement("SELECT 1;", block=True)
    assert not findings


def test_ddl_dml_detection():
    findings = check_ddl_dml("DELETE FROM foo")
    assert findings
    assert findings[0].code == "DDL_DML"


def test_bytes_thresholds():
    findings = check_bytes(600, warn=500, block=1000)
    assert findings
    assert findings[0].severity == Severity.WARN
    findings = check_bytes(1200, warn=500, block=1000)
    assert findings[0].severity == Severity.ERROR
