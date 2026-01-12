from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Optional

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, RichLog

from bq_guard.policy.types import Finding, Severity


@dataclass
class ReviewData:
    bytes_human: str
    bytes_processed: int | None
    findings: List[Finding]
    referenced_tables: List[str]
    partition_summary: List[str]


class ReviewModal(ModalScreen[bool]):
    def __init__(
        self,
        data: ReviewData,
        allow_execute_with_warnings: bool,
        on_execute: Callable[[], None],
    ) -> None:
        super().__init__()
        self.data = data
        self.allow_execute_with_warnings = allow_execute_with_warnings
        self.on_execute = on_execute
        self.confirm_input = Input(placeholder="RUN <bytes>")
        self.execute_button = Button("Execute", id="execute", disabled=True)
        self.cancel_button = Button("Close", id="close")

    def compose(self) -> ComposeResult:
        log = RichLog(markup=True)
        log.write(f"[b]Estimated bytes:[/b] {self.data.bytes_human}")
        log.write("\n[b]Findings:[/b]")
        for finding in self.data.findings:
            color = "red" if finding.severity == Severity.ERROR else "yellow"
            log.write(
                f"[{color}]{finding.severity} {finding.code}[/]: {finding.message}"
            )
        if not self.data.findings:
            log.write("- none")
        log.write("\n[b]Referenced tables:[/b]")
        for table in self.data.referenced_tables:
            log.write(f"- {table}")
        if not self.data.referenced_tables:
            log.write("- unknown")
        log.write("\n[b]Partition check:[/b]")
        for line in self.data.partition_summary:
            log.write(f"- {line}")
        if not self.data.partition_summary:
            log.write("- none")
        yield Vertical(
            Label("Review & Approve"),
            log,
            Label(f"Type confirmation: RUN {self.data.bytes_human}"),
            self.confirm_input,
            self.execute_button,
            self.cancel_button,
        )

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input is not self.confirm_input:
            return
        required = f"RUN {self.data.bytes_human}"
        has_error = any(f.severity == Severity.ERROR for f in self.data.findings)
        if has_error:
            self.execute_button.disabled = True
            return
        if not self.allow_execute_with_warnings and any(
            f.severity == Severity.WARN for f in self.data.findings
        ):
            self.execute_button.disabled = True
            return
        self.execute_button.disabled = event.value.strip() != required

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close":
            self.dismiss(False)
            return
        if event.button.id == "execute":
            self.on_execute()
            self.dismiss(True)


class SettingsModal(ModalScreen[Optional[dict]]):
    def __init__(self, config: dict) -> None:
        super().__init__()
        self.config = config
        self.warn_input = Input(value=str(config["limits"]["warn_bytes"]))
        self.block_input = Input(value=str(config["limits"]["block_bytes"]))
        self.preview_input = Input(value=str(config["app"]["preview_rows"]))
        self.location_input = Input(
            value=str(config["app"]["default_location"] or "")
        )
        self.save_button = Button("Save", id="save")
        self.cancel_button = Button("Cancel", id="cancel")

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Settings"),
            Label("warn_bytes"),
            self.warn_input,
            Label("block_bytes"),
            self.block_input,
            Label("preview_rows"),
            self.preview_input,
            Label("default_location"),
            self.location_input,
            self.save_button,
            self.cancel_button,
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel":
            self.dismiss(None)
            return
        if event.button.id == "save":
            try:
                warn = int(self.warn_input.value)
                block = int(self.block_input.value)
                preview = int(self.preview_input.value)
            except ValueError:
                self.dismiss(None)
                return
            self.config["limits"]["warn_bytes"] = max(0, warn)
            self.config["limits"]["block_bytes"] = max(0, block)
            self.config["app"]["preview_rows"] = max(1, preview)
            location = self.location_input.value.strip() or None
            self.config["app"]["default_location"] = location
            self.dismiss(self.config)


class ExportModal(ModalScreen[str | None]):
    def __init__(self) -> None:
        super().__init__()
        self.preview_button = Button("Export Preview", id="preview")
        self.all_button = Button("Export All", id="all")
        self.cancel_button = Button("Cancel", id="cancel")

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Export CSV"),
            self.preview_button,
            self.all_button,
            self.cancel_button,
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel":
            self.dismiss(None)
            return
        if event.button.id == "preview":
            self.dismiss("preview")
        elif event.button.id == "all":
            self.dismiss("all")
