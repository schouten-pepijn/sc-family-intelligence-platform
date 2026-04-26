"""Shared helpers used across command modules."""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

from fip.gold.cbs.cbs_reference_codes_writer import CBSReferenceCodeWriter
from fip.ingestion.base import RawRecord, Source
from fip.raw.reader import S3RawSnapshotReader, RawSnapshotReader
from fip.readback.duckdb import attach_iceberg_catalog, load_extensions
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
    run_id: str | None = None,
) -> list[dict[str, object]]:
    conn = connect_duckdb()
    try:
        load_extensions(conn)
        attach_iceberg_catalog(conn)
        ns = namespace or get_settings().bronze_namespace
        query = f"SELECT * FROM iceberg_catalog.{ns}.{table_name}"
        params: list[object] = []
        if run_id is not None:
            query += " WHERE run_id = ?"
            params.append(run_id)
        return conn.execute(query, params).to_arrow_table().to_pylist()
    finally:
        conn.close()


def read_silver_rows(
    table_name: str,
    namespace: str | None = None,
    run_id: str | None = None,
) -> list[dict[str, object]]:
    conn = connect_duckdb()
    try:
        load_extensions(conn)
        attach_iceberg_catalog(conn)
        ns = namespace or get_settings().silver_namespace
        query = f"SELECT * FROM iceberg_catalog.{ns}.{table_name}"
        params: list[object] = []
        if run_id is not None:
            query += " WHERE run_id = ?"
            params.append(run_id)
        return conn.execute(query, params).to_arrow_table().to_pylist()
    finally:
        conn.close()


def build_gold_reference_codes(
    table_id: str,
    run_id: str,
    entity: str,
    table_name: str,
    raw_target: str = "s3",
    raw_output_dir: Path = Path(".raw"),
) -> int:
    reader: RawSnapshotReader | S3RawSnapshotReader
    if raw_target == "local":
        reader = RawSnapshotReader(base_dir=raw_output_dir)
    elif raw_target == "s3":
        reader = S3RawSnapshotReader()
    else:
        raise ValueError("raw_target must be either 'local' or 's3'")

    records = list(reader.iter_cbs_entity_records(table_id=table_id, run_id=run_id, entity=entity))
    sink = CBSReferenceCodeWriter(table_name=table_name, entity=entity)
    return sink.write(records)


def dedupe_raw_records(records: list[RawRecord]) -> list[RawRecord]:
    """Keep the last record per source/entity/natural_key within a batch."""
    deduped: dict[str, RawRecord] = {}
    for record in records:
        key = f"{record.source_name}:{record.entity_name}:{record.natural_key}"
        deduped[key] = record
    return list(deduped.values())
