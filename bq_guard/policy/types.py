from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Severity(str, Enum):
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"


@dataclass
class Finding:
    severity: Severity
    code: str
    message: str
    evidence: Optional[str] = None
    table: Optional[str] = None
