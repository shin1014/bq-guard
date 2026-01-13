from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from google.cloud import bigquery


class DryRunResult:
    def __init__(
        self,
        bytes_processed: Optional[int],
        referenced_tables: List[str],
        job: bigquery.QueryJob,
    ) -> None:
        self.bytes_processed = bytes_processed
        self.referenced_tables = referenced_tables
        self.job = job


def _extract_referenced_tables(job: bigquery.QueryJob) -> List[str]:
    tables: List[str] = []
    try:
        api_repr = job.to_api_repr()
        stats = api_repr.get("statistics", {}).get("query", {})
        referenced = stats.get("referencedTables", [])
        for item in referenced:
            project = item.get("projectId")
            dataset = item.get("datasetId")
            table = item.get("tableId")
            if project and dataset and table:
                tables.append(f"{project}.{dataset}.{table}")
    except Exception:
        return tables
    return tables


def dry_run_query(
    client: bigquery.Client,
    sql: str,
    location: str,
    labels: Dict[str, str],
) -> DryRunResult:
    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
        labels=labels,
    )
    job = client.query(sql, job_config=job_config, location=location)
    bytes_processed = getattr(job, "total_bytes_processed", None)
    referenced_tables = _extract_referenced_tables(job)
    return DryRunResult(bytes_processed, referenced_tables, job)


def execute_query(
    client: bigquery.Client,
    sql: str,
    location: str,
    labels: Dict[str, str],
    use_query_cache: bool,
) -> bigquery.QueryJob:
    job_config = bigquery.QueryJobConfig(
        dry_run=False,
        use_query_cache=use_query_cache,
        labels=labels,
    )
    job = client.query(sql, job_config=job_config, location=location)
    return job


def fetch_preview(
    job: bigquery.QueryJob, max_rows: int
) -> Tuple[List[str], List[List[Any]]]:
    result = job.result(max_results=max_rows)
    fields = [field.name for field in result.schema]
    rows = [[row.get(field) for field in fields] for row in result]
    return fields, rows


def fetch_paged(
    job: bigquery.QueryJob, page_size: int
) -> Iterable[Tuple[List[str], List[List[Any]]]]:
    iterator = job.result(page_size=page_size)
    schema = [field.name for field in iterator.schema]
    for page in iterator.pages:
        rows = [[row.get(field) for field in schema] for row in page]
        yield schema, rows


def export_all_to_csv(
    job: bigquery.QueryJob,
    output_path: str,
    page_size: int,
    progress_cb: Optional[callable] = None,
) -> int:
    iterator = job.result(page_size=page_size)
    fields = [field.name for field in iterator.schema]
    row_count = 0
    with open(output_path, "w", encoding="utf-8", newline="") as handle:
        handle.write(",".join(fields) + "\n")
        for page in iterator.pages:
            for row in page:
                handle.write(",".join(str(row.get(field, "")) for field in fields) + "\n")
                row_count += 1
                if progress_cb and row_count % page_size == 0:
                    progress_cb(row_count)
    return row_count
