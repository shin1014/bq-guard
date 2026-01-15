from __future__ import annotations

from google.cloud import bigquery


def get_client(project: str | None = None) -> bigquery.Client:
    return bigquery.Client(project=project)
