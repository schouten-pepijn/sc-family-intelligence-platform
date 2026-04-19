"""Shared helpers used across command modules."""

from __future__ import annotations

from typing import Iterator

from fip.gold.cbs.cbs_reference_codes_writer import CBSReferenceCodeWriter
from fip.ingestion.base import RawRecord, Source
from fip.ingestion.cbs.adapter import CBSODataSource
from fip.readback.duckdb import attach_lakekeeper_catalog, load_extensions
from fip.readback.duckdb import connect as connect_duckdb
from fip.settings import get_settings


def iter_sampled_records(
    source: Source,
    entity: str | None,
    limit: int,
) -> Iterator[RawRecord]:
    """Yield up to `limit` records from a source, optionally filtering by entity suffix."""
    count = 0
    for record in source.iter_records():
        if entity is not None and not record.entity_name.endswith(f".{entity}"):
            continue
        yield record
        count += 1
        if count >= limit:
            break


def iter_records_for_entity(source: Source, entity: str) -> Iterator[RawRecord]:
    """Yield only records matching the given entity suffix."""
    for record in source.iter_records():
        if record.entity_name.endswith(f".{entity}"):
            yield record


def read_bronze_rows(
    table_name: str,
    namespace: str | None = None,
) -> list[dict[str, object]]:
    conn = connect_duckdb()
    try:
        load_extensions(conn)
        attach_lakekeeper_catalog(conn)
        ns = namespace or get_settings().bronze_namespace
        query = f"SELECT * FROM lakekeeper_catalog.{ns}.{table_name}"
        return conn.execute(query).to_arrow_table().to_pylist()
    finally:
        conn.close()


def read_silver_rows(
    table_name: str,
    namespace: str | None = None,
) -> list[dict[str, object]]:
    conn = connect_duckdb()
    try:
        load_extensions(conn)
        attach_lakekeeper_catalog(conn)
        ns = namespace or get_settings().silver_namespace
        query = f"SELECT * FROM lakekeeper_catalog.{ns}.{table_name}"
        return conn.execute(query).to_arrow_table().to_pylist()
    finally:
        conn.close()


def build_gold_reference_codes(
    table_id: str,
    run_id: str,
    entity: str,
    table_name: str,
) -> int:
    source = CBSODataSource(table_id=table_id, run_id=run_id)
    records = list(iter_records_for_entity(source=source, entity=entity))
    sink = CBSReferenceCodeWriter(table_name=table_name, entity=entity)
    return sink.write(records)
