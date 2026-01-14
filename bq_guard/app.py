from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    RichLog,
    TabbedContent,
    TabPane,
    TextArea,
)

from bq_guard.bq.client import get_client
from bq_guard.bq.jobs import DryRunResult, execute_query, fetch_paged, fetch_preview
from bq_guard.bq.metadata import extract_partition_info, fetch_table_metadata
from bq_guard.cache import TableMeta, TableMetaCache
from bq_guard.config import load_config, save_config
from bq_guard.gcloud import get_default_location, get_default_project
from bq_guard.history import append_history
from bq_guard.policy.checks import run_policy_checks
from bq_guard.policy.partition import enforce_partition_filters
from bq_guard.policy.types import Finding, Severity
from bq_guard.ui.modals import ExportModal, ReviewData, ReviewModal, SettingsModal
from bq_guard.ui.panels import EstimatePanel, EstimateState
from bq_guard.ui.results import PagedResultView

TABLE_REGEX = re.compile(r"`?([\w-]+)\.([\w-]+)\.([\w-]+)`?")
DATASET_TABLE_REGEX = re.compile(r"`?([\w-]+)\.([\w-]+)`?")


@dataclass
class DryRunState:
    sql: str
    bytes_processed: Optional[int]
    referenced_tables: List[str]
    findings: List[Finding]
    partition_summary: List[str]


class BqGuardApp(App):
    CSS = """
    Screen {
        layout: vertical;
    }
    #body {
        height: 1fr;
    }
    #editor {
        height: 1fr;
    }
    #estimate {
        width: 35%;
        min-width: 30;
    }
    #bottom {
        height: 40%;
    }
    """

    BINDINGS = [
        ("ctrl+e", "estimate", "Estimate"),
        ("ctrl+enter", "review", "Review"),
        ("ctrl+s", "export", "Export"),
        ("ctrl+comma", "settings", "Settings"),
        ("ctrl+m", "refresh_meta", "Refresh metadata"),
        ("ctrl+q", "quit", "Quit"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.config_result = load_config()
        self.config = self.config_result.config
        self._log = RichLog(markup=True)
        self._editor = TextArea(id="editor")
        self._estimate_panel = EstimatePanel()
        self._preview_table = DataTable()
        self._paged_view = PagedResultView()
        self._estimate_task: Optional[asyncio.Task] = None
        self._revision = 0
        self._last_dry_run: Optional[DryRunState] = None
        self._last_job: Optional[bigquery.QueryJob] = None
        self._preview_rows: List[List[str]] = []
        self._preview_columns: List[str] = []
        self._client: Optional[bigquery.Client] = None
        self._cache = TableMetaCache(self.config["cache"]["schema_version"])
        self._cache.load()
        self._default_project = self.config["app"].get("default_project") or get_default_project()
        self._default_location = (
            self.config["app"].get("default_location") or get_default_location() or "asia-northeast1"
        )

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="body"):
            yield self._editor
            yield self._estimate_panel
        with TabbedContent(id="bottom"):
            with TabPane("Preview"):
                yield self._preview_table
            with TabPane("All"):
                yield self._paged_view
            with TabPane("Logs"):
                yield self._log
        yield Footer()

    def on_mount(self) -> None:
        self._client = get_client(self._default_project)
        for warning in self.config_result.warnings:
            self._log.write(f"[yellow]Config warning:[/] {warning}")
        self._update_estimate_panel([])

    def action_estimate(self) -> None:
        self._schedule_estimate()

    def action_review(self) -> None:
        asyncio.create_task(self._review_flow())

    def action_export(self) -> None:
        self.push_screen(ExportModal(), self._handle_export_choice)

    def action_settings(self) -> None:
        self.push_screen(SettingsModal(self.config), self._handle_settings_save)

    def action_refresh_meta(self) -> None:
        self._cache.tables.clear()
        self._cache.save()
        self._log.write("[yellow]Metadata cache cleared. Next dry-run will refetch.[/]")
        self._schedule_estimate()

    def on_text_area_changed(self, event: TextArea.Changed) -> None:
        if event.text_area is not self._editor:
            return
        self._revision += 1
        self._schedule_estimate()

    def _schedule_estimate(self) -> None:
        if self._estimate_task and not self._estimate_task.done():
            self._estimate_task.cancel()
        delay = self.config["ui"].get("auto_estimate_debounce_ms", 900) / 1000
        revision = self._revision
        self._estimate_task = asyncio.create_task(self._debounced_estimate(delay, revision))

    async def _debounced_estimate(self, delay: float, revision: int) -> None:
        await asyncio.sleep(delay)
        if revision != self._revision:
            return
        await self._run_dry_run(revision)

    def _update_estimate_panel(self, findings: List[Finding]) -> None:
        state = EstimateState(
            project=self._default_project or "-",
            location=self._default_location,
            bytes_processed=self._last_dry_run.bytes_processed if self._last_dry_run else None,
            findings=findings,
            partition_summary=self._last_dry_run.partition_summary if self._last_dry_run else [],
            last_updated=datetime.now(timezone.utc).isoformat(),
        )
        self._estimate_panel.update_state(state)

    async def _run_dry_run(self, revision: int) -> Optional[DryRunState]:
        sql = self._editor.text
        if not sql.strip():
            return None
        client = self._client
        if client is None:
            return None
        labels = dict(self.config["bq"].get("labels", {}))
        labels["mode"] = "dry-run"
        try:
            dry_result = DryRunResultWrapper.from_result(
                await asyncio.to_thread(
                    self._dry_run_wrapper,
                    client,
                    sql,
                    labels,
                )
            )
        except Exception as exc:
            self._log.write(f"[red]Dry-run failed:[/] {exc}")
            append_history(
                {
                    "project": self._default_project,
                    "location": self._default_location,
                    "sql": sql,
                    "status": "DRYRUN_FAILED",
                }
            )
            self._last_dry_run = DryRunState(sql, None, [], [], [])
            self._update_estimate_panel([])
            return None
        if revision != self._revision:
            return None
        referenced_tables = dry_result.referenced_tables
        if not referenced_tables:
            referenced_tables = self._extract_tables(sql)
        table_meta = await asyncio.to_thread(self._ensure_table_metadata, referenced_tables)
        findings = run_policy_checks(sql, self.config, dry_result.bytes_processed)
        partition_findings: List[Finding] = []
        partition_summary: List[str] = []
        if self.config["policy"].get("enforce_partition_filter", True):
            partition_findings, partition_summary = enforce_partition_filters(
                sql, referenced_tables, table_meta, self.config
            )
        findings.extend(partition_findings)
        self._last_dry_run = DryRunState(
            sql=sql,
            bytes_processed=dry_result.bytes_processed,
            referenced_tables=referenced_tables,
            findings=findings,
            partition_summary=partition_summary,
        )
        self._update_estimate_panel(findings)
        self._log.write("Dry-run updated")
        return self._last_dry_run

    def _dry_run_wrapper(
        self, client: bigquery.Client, sql: str, labels: Dict[str, str]
    ) -> DryRunResult:
        from bq_guard.bq.jobs import dry_run_query

        return dry_run_query(client, sql, self._default_location, labels)

    def _ensure_table_metadata(self, tables: List[str]) -> Dict[str, TableMeta]:
        cache_updated = False
        result: Dict[str, TableMeta] = {}
        if self._client is None:
            return result
        for table in tables:
            cached = self._cache.get(table)
            if cached:
                result[table] = cached
                continue
            try:
                project, dataset, table_name = table.split(".")
                table_ref = bigquery.TableReference(
                    bigquery.DatasetReference(project, dataset), table_name
                )
                table_obj = fetch_table_metadata(self._client, table_ref)
                meta = extract_partition_info(table_obj)
                meta.last_seen_ts = self._cache.now_ts()
                self._cache.update(table, meta)
                cache_updated = True
                result[table] = meta
            except Exception as exc:
                self._log.write(f"[red]Metadata fetch failed:[/] {table} {exc}")
        if cache_updated:
            try:
                self._cache.save()
            except Exception as exc:
                self._log.write(f"[yellow]Cache save failed:[/] {exc}")
        return result

    def _extract_tables(self, sql: str) -> List[str]:
        tables = []
        for match in TABLE_REGEX.finditer(sql):
            project, dataset, table = match.groups()
            tables.append(f"{project}.{dataset}.{table}")
        if tables:
            return list(dict.fromkeys(tables))
        if self._default_project:
            for match in DATASET_TABLE_REGEX.finditer(sql):
                dataset, table = match.groups()
                tables.append(f"{self._default_project}.{dataset}.{table}")
        return list(dict.fromkeys(tables))

    async def _review_flow(self) -> None:
        dry_state = await self._run_dry_run(self._revision)
        if dry_state is None:
            return
        bytes_human = human_bytes(dry_state.bytes_processed or 0)
        data = ReviewData(
            bytes_human=bytes_human,
            bytes_processed=dry_state.bytes_processed,
            findings=dry_state.findings,
            referenced_tables=dry_state.referenced_tables,
            partition_summary=dry_state.partition_summary,
        )
        has_error = any(f.severity == Severity.ERROR for f in dry_state.findings)
        findings_payload = [serialize_finding(f) for f in dry_state.findings]
        if has_error:
            append_history(
                {
                    "project": self._default_project,
                    "location": self._default_location,
                    "sql": dry_state.sql,
                    "dry_run_bytes": dry_state.bytes_processed,
                    "referenced_tables": dry_state.referenced_tables,
                    "findings": findings_payload,
                    "status": "BLOCKED",
                }
            )
        else:
            append_history(
                {
                    "project": self._default_project,
                    "location": self._default_location,
                    "sql": dry_state.sql,
                    "dry_run_bytes": dry_state.bytes_processed,
                    "referenced_tables": dry_state.referenced_tables,
                    "findings": findings_payload,
                    "status": "REVIEWED",
                }
            )

        def _execute() -> None:
            asyncio.create_task(self._execute_query(dry_state.sql))

        self.push_screen(
            ReviewModal(
                data,
                self.config["policy"].get("allow_execute_with_warnings", True),
                _execute,
            )
        )

    async def _execute_query(self, sql: str) -> None:
        client = self._client
        if client is None:
            return
        labels = dict(self.config["bq"].get("labels", {}))
        labels["mode"] = "execute"
        try:
            job = await asyncio.to_thread(
                execute_query,
                client,
                sql,
                self._default_location,
                labels,
                self.config["bq"].get("use_query_cache", False),
            )
            self._last_job = job
            append_history(
                {
                    "project": self._default_project,
                    "location": self._default_location,
                    "sql": sql,
                    "job_id": job.job_id,
                    "status": "EXECUTED",
                }
            )
            result = await asyncio.to_thread(job.result)
            self._log.write(f"[green]Execution complete:[/] {job.job_id}")
            await self._load_preview(job)
            self._paged_view.set_iterator(fetch_paged(job, self.config["app"]["page_size"]))
        except Exception as exc:
            self._log.write(f"[red]Execution failed:[/] {exc}")
            append_history(
                {
                    "project": self._default_project,
                    "location": self._default_location,
                    "sql": sql,
                    "status": "EXEC_FAILED",
                }
            )

    async def _load_preview(self, job: bigquery.QueryJob) -> None:
        columns, rows = await asyncio.to_thread(
            fetch_preview, job, self.config["app"]["preview_rows"]
        )
        self._preview_columns = columns
        self._preview_rows = rows
        self._preview_table.clear(columns=True)
        if columns:
            self._preview_table.add_columns(*columns)
        for row in rows:
            self._preview_table.add_row(*[str(value) for value in row])

    def _handle_settings_save(self, result: Optional[dict]) -> None:
        if result is None:
            return
        self.config = result
        save_config(self.config)
        self._default_location = (
            self.config["app"].get("default_location") or get_default_location() or "asia-northeast1"
        )
        self._log.write("[green]Settings saved.[/]")
        self._schedule_estimate()

    def _handle_export_choice(self, choice: Optional[str]) -> None:
        if choice is None:
            return
        if choice == "preview":
            self._export_preview()
        elif choice == "all":
            asyncio.create_task(self._export_all())

    def _export_preview(self) -> None:
        if not self._preview_columns:
            self._log.write("[yellow]No preview data to export.[/]")
            return
        output_path = self._export_path("preview")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            handle.write(",".join(self._preview_columns) + "\n")
            for row in self._preview_rows:
                handle.write(",".join(str(value) for value in row) + "\n")
        self._log.write(f"[green]Preview exported:[/] {output_path}")
        append_history(
            {
                "project": self._default_project,
                "location": self._default_location,
                "sql": self._editor.text,
                "status": "EXPORTED",
                "exported_files": [str(output_path)],
            }
        )

    async def _export_all(self) -> None:
        if not self._last_job:
            self._log.write("[yellow]No execution result to export.[/]")
            return
        output_path = self._export_path("all")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        def progress(count: int) -> None:
            self._log.write(f"Exported {count} rows...")

        try:
            from bq_guard.bq.jobs import export_all_to_csv

            total = await asyncio.to_thread(
                export_all_to_csv,
                self._last_job,
                str(output_path),
                self.config["app"]["page_size"],
                progress,
            )
            self._log.write(f"[green]All exported:[/] {output_path} ({total} rows)")
            append_history(
                {
                    "project": self._default_project,
                    "location": self._default_location,
                    "sql": self._editor.text,
                    "status": "EXPORTED",
                    "exported_files": [str(output_path)],
                }
            )
        except Exception as exc:
            self._log.write(f"[red]Export failed:[/] {exc}")

    def _export_path(self, kind: str) -> Path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_id = self._last_job.job_id if self._last_job else "nojob"
        return Path("exports") / f"{ts}_{job_id}_{kind}.csv"


@dataclass
class DryRunResultWrapper:
    bytes_processed: Optional[int]
    referenced_tables: List[str]

    @classmethod
    def from_result(cls, result: DryRunResult) -> "DryRunResultWrapper":
        return cls(result.bytes_processed, result.referenced_tables)


def human_bytes(value: int) -> str:
    if value < 1024:
        return f"{value}B"
    for unit in ["KB", "MB", "GB", "TB", "PB"]:
        value /= 1024
        if value < 1024:
            return f"{value:.1f}{unit}"
    return f"{value:.1f}EB"


def serialize_finding(finding: Finding) -> dict:
    return {
        "severity": finding.severity.value,
        "code": finding.code,
        "message": finding.message,
        "evidence": finding.evidence,
        "table": finding.table,
    }


def main() -> None:
    app = BqGuardApp()
    app.run()
