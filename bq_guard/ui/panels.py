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
        self._log_widget: RichLog | None = None

    def compose(self):
        self._log_widget = RichLog(markup=True)
        yield self._log_widget

    def update_state(self, state: EstimateState) -> None:
        if self._log_widget is None:
            return
        self._log_widget.clear()
        self._log_widget.write(f"[b]Project:[/b] {state.project}")
        self._log_widget.write(f"[b]Location:[/b] {state.location}")
        bytes_value = "-" if state.bytes_processed is None else f"{state.bytes_processed:,}"
        self._log_widget.write(f"[b]Estimated bytes:[/b] {bytes_value}")
        if state.last_updated:
            self._log_widget.write(f"[b]Updated:[/b] {state.last_updated}")
        self._log_widget.write("\n[b]Findings:[/b]")
        if not state.findings:
            self._log_widget.write("- none")
        for finding in state.findings:
            color = "red" if finding.severity == Severity.ERROR else "yellow"
            evidence = f" ({finding.evidence})" if finding.evidence else ""
            table = f" [{finding.table}]" if finding.table else ""
            self._log_widget.write(
                f"[{color}]{finding.severity} {finding.code}[/]: {finding.message}{evidence}{table}"
            )
        self._log_widget.write("\n[b]Partition check:[/b]")
        if not state.partition_summary:
            self._log_widget.write("- none")
        else:
            for line in state.partition_summary:
                self._log_widget.write(f"- {line}")
