# ruff: noqa: F401
import json
from pathlib import Path
from typing import Iterator

import typer

from fip.gold.cbs.cbs_observations_writer import CBSObservationLandingWriter
from fip.gold.cbs.cbs_reference_codes_writer import CBSReferenceCodeWriter
from fip.gold.core.service import write_rows_to_sink
from fip.gold.pdok_bag.bag_pand_writer import BAGPandLandingWriter
from fip.gold.pdok_bag.bag_verblijfsobject_writer import BAGVerblijfsobjectLandingWriter
from fip.gold.readback import connect as connect_postgres
from fip.gold.readback import count_rows as count_gold_rows
from fip.gold.readback import sample_rows as sample_gold_rows
from fip.ingestion.base import RawRecord
from fip.ingestion.cbs.adapter import CBSODataSource
from fip.ingestion.pdok_bag.adapter import PDOKBAGSource
from fip.lakehouse.bronze.bag_factory import BAGIcebergSinkFactory
from fip.lakehouse.bronze.cbs_factory import CBSIcebergSinkFactory
from fip.lakehouse.silver.cbs.cbs_observations_service import (
    write_bronze_rows_to_cbs_observation_sink,
)
from fip.lakehouse.silver.cbs.cbs_observations_sink import CBSObservationSink
from fip.lakehouse.silver.pdok_bag.bag_pand_service import (
    write_bronze_rows_to_bag_pand_sink,
)
from fip.lakehouse.silver.pdok_bag.bag_pand_sink import BAGPandSink
from fip.lakehouse.silver.pdok_bag.bag_verblijfsobject_service import (
    write_bronze_rows_to_bag_verblijfsobject_sink,
)
from fip.lakehouse.silver.pdok_bag.bag_verblijfsobject_sink import BAGVerblijfsobjectSink
from fip.raw.writer import MinioRawSnapshotWriter, RawSnapshotWriter
from fip.readback.duckdb import (
    attach_lakekeeper_catalog,
    count_rows,
    load_extensions,
    sample_rows,
)
from fip.readback.duckdb import connect as connect_duckdb
from fip.settings import get_settings

app = typer.Typer(help="Family Intelligence Platform CLI.")


@app.callback()
def main_callback() -> None:
    """Family Intelligence Platform command group."""
    return None


def iter_sampled_cbs_raw_records(
    source: CBSODataSource,
    entity: str | None,
    limit: int,
) -> Iterator[RawRecord]:
    count = 0
    for record in source.iter_records():
        if entity is not None and not record.entity_name.endswith(f".{entity}"):
            continue
        yield record
        count += 1
        if count >= limit:
            break


def iter_sampled_bag_raw_records(
    source: PDOKBAGSource,
    entity: str | None,
    limit: int,
) -> Iterator[RawRecord]:
    count = 0
    for record in source.iter_records():
        if entity is not None and not record.entity_name.endswith(f".{entity}"):
            continue
        yield record
        count += 1
        if count >= limit:
            break


def _iter_cbs_records_for_entity(
    source: CBSODataSource,
    entity: str,
) -> Iterator[RawRecord]:
    for record in source.iter_records():
        if record.entity_name.endswith(f".{entity}"):
            yield record


def _read_bronze_rows(
    table_name: str,
    namespace: str | None = None,
) -> list[dict[str, object]]:
    conn = connect_duckdb()

    try:
        load_extensions(conn)
        attach_lakekeeper_catalog(conn)

        query = (
            "SELECT * "
            f"FROM lakekeeper_catalog.{namespace or get_settings().bronze_namespace}.{table_name}"
        )
        return conn.execute(query).to_arrow_table().to_pylist()
    finally:
        conn.close()


def _build_gold_reference_codes(
    table_id: str,
    run_id: str,
    entity: str,
    table_name: str,
) -> None:
    source = CBSODataSource(table_id=table_id, run_id=run_id)
    records = list(_iter_cbs_records_for_entity(source=source, entity=entity))
    sink = CBSReferenceCodeWriter(table_name=table_name, entity=entity)

    written = sink.write(records)
    typer.echo(f"Wrote {written} {entity} rows into {table_name}")


def _read_silver_rows(
    table_name: str,
    namespace: str | None = None,
) -> list[dict[str, object]]:
    conn = connect_duckdb()

    try:
        load_extensions(conn)
        attach_lakekeeper_catalog(conn)

        query = (
            "SELECT * "
            f"FROM lakekeeper_catalog.{namespace or get_settings().silver_namespace}.{table_name}"
        )
        return conn.execute(query).to_arrow_table().to_pylist()
    finally:
        conn.close()


def main() -> None:
    app()


from fip.commands import cbs as _cbs  # noqa: E402,F401
from fip.commands import inspection as _inspection  # noqa: E402,F401
from fip.commands import pdok_bag as _pdok_bag  # noqa: E402,F401
