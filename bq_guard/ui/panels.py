from __future__ import annotations

from dataclasses import dataclass
from typing import List

from textual.widget import Widget
from textual.widgets import RichLog

from bq_guard.policy.types import Finding, Severity


@dataclass
class EstimateState:
    project: str
    location: str
    bytes_processed: int | None
    findings: List[Finding]
    partition_summary: List[str]
    last_updated: str | None = None


class EstimatePanel(Widget):
    def __init__(self) -> None:
        super().__init__()
        self.log = RichLog(markup=True)

    def compose(self):
        yield self.log

    def update_state(self, state: EstimateState) -> None:
        self.log.clear()
        self.log.write(f"[b]Project:[/b] {state.project}")
        self.log.write(f"[b]Location:[/b] {state.location}")
        bytes_value = "-" if state.bytes_processed is None else f"{state.bytes_processed:,}"
        self.log.write(f"[b]Estimated bytes:[/b] {bytes_value}")
        if state.last_updated:
            self.log.write(f"[b]Updated:[/b] {state.last_updated}")
        self.log.write("\n[b]Findings:[/b]")
        if not state.findings:
            self.log.write("- none")
        for finding in state.findings:
            color = "red" if finding.severity == Severity.ERROR else "yellow"
            evidence = f" ({finding.evidence})" if finding.evidence else ""
            table = f" [{finding.table}]" if finding.table else ""
            self.log.write(
                f"[{color}]{finding.severity} {finding.code}[/]: {finding.message}{evidence}{table}"
            )
        self.log.write("\n[b]Partition check:[/b]")
        if not state.partition_summary:
            self.log.write("- none")
        else:
            for line in state.partition_summary:
                self.log.write(f"- {line}")
