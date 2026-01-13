from __future__ import annotations

import re

SINGLE_LINE_COMMENT = re.compile(r"--.*?$", re.MULTILINE)
MULTI_LINE_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
STRING_LITERAL = re.compile(r"'(?:''|[^'])*'")
BACKTICK_LITERAL = re.compile(r"`[^`]*`")


def strip_comments(sql: str) -> str:
    sql = SINGLE_LINE_COMMENT.sub("", sql)
    sql = MULTI_LINE_COMMENT.sub("", sql)
    return sql


def strip_strings(sql: str) -> str:
    sql = STRING_LITERAL.sub("''", sql)
    sql = BACKTICK_LITERAL.sub("``", sql)
    return sql


def sanitize_sql(sql: str) -> str:
    return strip_strings(strip_comments(sql))
