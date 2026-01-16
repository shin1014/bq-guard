from __future__ import annotations

from typing import Optional

from google.cloud import bigquery

from bq_guard.cache import TableMeta


def extract_partition_info(table: bigquery.Table) -> TableMeta:
    partition_type = "none"
    partition_key: Optional[str] = None
    ingestion_time = False

    if table.time_partitioning:
        partition_type = "time"
        if table.time_partitioning.field:
            partition_key = table.time_partitioning.field
        else:
            partition_key = "_PARTITIONDATE"
            ingestion_time = True
    if table.range_partitioning:
        partition_type = "range"
        partition_key = table.range_partitioning.field

    return TableMeta(
        partition_type=partition_type,
        partition_key=partition_key,
        ingestion_time=ingestion_time,
        last_seen_ts="",
    )


def fetch_table_metadata(
    client: bigquery.Client, table_ref: bigquery.TableReference
) -> bigquery.Table:
    return client.get_table(table_ref)
