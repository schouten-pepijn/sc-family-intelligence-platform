from __future__ import annotations

from pathlib import Path
from typing import TextIO

import typer

from fip.cli import app
from fip.commands._helpers import dedupe_raw_records, read_bronze_rows, read_silver_rows
from fip.gold.core.service import write_rows_to_sink
from fip.gold.pdok_bag.bag_adressen_writer import BAGAdressenLandingWriter
from fip.gold.pdok_bag.bag_pand_writer import BAGPandLandingWriter
from fip.gold.pdok_bag.bag_verblijfsobject_writer import BAGVerblijfsobjectLandingWriter
from fip.ingestion.base import RawRecord
from fip.ingestion.pdok_bag.gpkg_source import PDOKBAGGeoPackageSource
from fip.ingestion.pdok_bag.adapter import PDOKBAGSource
from fip.lakehouse.bronze.bag_factory import BAGIcebergSinkFactory
from fip.lakehouse.silver.pdok_bag.bag_adressen_service import (
    write_bronze_rows_to_bag_adressen_sink,
)
from fip.lakehouse.silver.pdok_bag.bag_adressen_sink import BAGAdressenSink
from fip.lakehouse.silver.pdok_bag.bag_pand_service import (
    write_bronze_rows_to_bag_pand_sink,
)
from fip.lakehouse.silver.pdok_bag.bag_pand_sink import BAGPandSink
from fip.lakehouse.silver.pdok_bag.bag_verblijfsobject_service import (
    write_bronze_rows_to_bag_verblijfsobject_sink,
)
from fip.lakehouse.silver.pdok_bag.bag_verblijfsobject_sink import (
    BAGVerblijfsobjectSink,
)
from fip.raw.reader import S3RawSnapshotReader, RawSnapshotReader
from fip.raw.writer import S3RawSnapshotWriter, RawSnapshotWriter, serialize_raw_record
from fip.settings import get_settings


def _bag_raw_reader(
    raw_target: str,
    raw_output_dir: Path,
) -> RawSnapshotReader | S3RawSnapshotReader:
    if raw_target == "local":
        return RawSnapshotReader(base_dir=raw_output_dir)
    if raw_target == "s3":
        return S3RawSnapshotReader()
    raise typer.BadParameter("raw_target must be either 'local' or 's3'")


@app.command("ingest-bag")
def ingest_bag(
    run_id: str = typer.Option(..., help="Run identifier for this ingestion."),
    collection: str = typer.Option(
        "verblijfsobject",
        help="BAG collection to ingest, for example verblijfsobject, pand, or adres.",
    ),
    target_namespace: str | None = typer.Option(
        None,
        help="Target Iceberg namespace.",
    ),
    limit: int = typer.Option(
        1000,
        min=1,
        help="Maximum number of BAG records to ingest.",
    ),
    progress_every: int = typer.Option(
        1000,
        min=1,
        help="Print progress every N records while reading BAG.",
    ),
    raw_target: str = typer.Option(
        "s3",
        help="Raw source target: local JSONL files or S3-compatible object storage.",
    ),
    raw_output_dir: Path = Path(".raw"),
) -> None:
    if target_namespace is None:
        target_namespace = get_settings().bronze_namespace

    reader = _bag_raw_reader(raw_target=raw_target, raw_output_dir=raw_output_dir)
    sink_factory = BAGIcebergSinkFactory(namespace=target_namespace)

    grouped_records: dict[str, list[RawRecord]] = {}
    seen = 0
    for record in reader.iter_bag_records(run_id=run_id, collection=collection):
        grouped_records.setdefault(record.entity_name, []).append(record)
        seen += 1
        if seen % progress_every == 0:
            typer.echo(f"Read {seen} BAG records...")
        if seen >= limit:
            break

    written = 0
    for entity_name, records in grouped_records.items():
        sink = sink_factory.for_entity(entity_name)
        written += sink.write(dedupe_raw_records(records))

    typer.echo(f"Wrote {written} records using sink namespace {target_namespace}")


@app.command("archive-bag-raw")
def archive_bag_raw(
    run_id: str = typer.Option("debug-raw"),
    collection: str = typer.Option(
        "verblijfsobject",
        help="BAG collection to archive, for example verblijfsobject, pand, or adres.",
    ),
    limit: int | None = typer.Option(
        None,
        help="Maximum number of raw records to archive. Leave unset for a full pull.",
    ),
    target: str = typer.Option(
        "s3",
        help="Raw storage target: local JSONL files or S3-compatible object storage.",
    ),
    output_dir: Path = Path(".raw"),
) -> None:
    source = PDOKBAGSource(run_id=run_id, collection=collection)

    writer: RawSnapshotWriter | S3RawSnapshotWriter
    if target == "local":
        writer = RawSnapshotWriter(base_dir=str(output_dir))
    elif target == "s3":
        writer = S3RawSnapshotWriter()
    else:
        raise typer.BadParameter("target must be either 'local' or 's3'")

    archived = 0
    expected_entity: str | None = None
    handle: TextIO | None = None

    try:
        for record in source.iter_records():
            if expected_entity is None:
                expected_entity = record.entity_name
                handle = writer.open_for_record(record)
            elif record.entity_name != expected_entity:
                raise ValueError("archive-bag-raw expects records for a single BAG collection")

            assert handle is not None
            handle.write(serialize_raw_record(record))
            handle.write("\n")
            archived += 1
            if limit is not None and archived >= limit:
                break
    finally:
        if handle is not None:
            handle.close()

    typer.echo(f"Wrote {archived} raw records")


@app.command("build-bag-silver-verblijfsobject")
def build_bag_silver_verblijfsobject(
    table_name: str = typer.Option(
        "bag_verblijfsobject",
        "--table",
        help="Bronze table name to transform into Silver.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Bronze Iceberg namespace. Defaults to configured bronze namespace.",
    ),
    run_id: str | None = typer.Option(
        None,
        help="Bronze run identifier to materialize into Silver.",
    ),
) -> None:
    bronze_rows = read_bronze_rows(table_name=table_name, namespace=namespace, run_id=run_id)
    silver_namespace = get_settings().silver_namespace
    sink = BAGVerblijfsobjectSink(table_ident=f"{silver_namespace}.bag_verblijfsobject_flat")

    written = write_bronze_rows_to_bag_verblijfsobject_sink(bronze_rows, sink)
    typer.echo(f"Wrote {written} BAG Silver rows")


@app.command("build-bag-silver-adressen")
def build_bag_silver_adressen(
    table_name: str = typer.Option(
        "bag_adressen",
        "--table",
        help="Bronze table name to transform into Silver.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Bronze Iceberg namespace. Defaults to configured bronze namespace.",
    ),
    run_id: str | None = typer.Option(
        None,
        help="Bronze run identifier to materialize into Silver.",
    ),
) -> None:
    bronze_rows = read_bronze_rows(table_name=table_name, namespace=namespace, run_id=run_id)
    silver_namespace = get_settings().silver_namespace
    sink = BAGAdressenSink(table_ident=f"{silver_namespace}.bag_adressen_flat")

    written = write_bronze_rows_to_bag_adressen_sink(bronze_rows, sink)
    typer.echo(f"Wrote {written} BAG Silver rows")


@app.command("build-bag-silver-pand")
def build_bag_silver_pand(
    table_name: str = typer.Option(
        "bag_pand",
        "--table",
        help="Bronze table name to transform into Silver.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Bronze Iceberg namespace. Defaults to configured bronze namespace.",
    ),
    run_id: str | None = typer.Option(
        None,
        help="Bronze run identifier to materialize into Silver.",
    ),
) -> None:
    bronze_rows = read_bronze_rows(table_name=table_name, namespace=namespace, run_id=run_id)
    silver_namespace = get_settings().silver_namespace
    sink = BAGPandSink(table_ident=f"{silver_namespace}.bag_pand_flat")

    written = write_bronze_rows_to_bag_pand_sink(bronze_rows, sink)
    typer.echo(f"Wrote {written} BAG Silver rows")


@app.command("build-bag-landing-verblijfsobject")
def build_bag_landing_verblijfsobject(
    table_name: str = typer.Option(
        "bag_verblijfsobject_flat",
        "--table",
        help="Silver table name to materialize into the Postgres landing layer.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Silver Iceberg namespace. Defaults to configured silver namespace.",
    ),
    run_id: str | None = typer.Option(
        None,
        help="Silver run identifier to materialize into the landing layer.",
    ),
) -> None:
    silver_rows = read_silver_rows(table_name=table_name, namespace=namespace, run_id=run_id)
    sink = BAGVerblijfsobjectLandingWriter(table_name="bag_verblijfsobject")

    written = write_rows_to_sink(silver_rows, sink)
    typer.echo(f"Wrote {written} BAG landing rows")


@app.command("build-bag-landing-adressen")
def build_bag_landing_adressen(
    table_name: str = typer.Option(
        "bag_adressen_flat",
        "--table",
        help="Silver table name to materialize into the Postgres landing layer.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Silver Iceberg namespace. Defaults to configured silver namespace.",
    ),
    run_id: str | None = typer.Option(
        None,
        help="Silver run identifier to materialize into the landing layer.",
    ),
) -> None:
    silver_rows = read_silver_rows(table_name=table_name, namespace=namespace, run_id=run_id)
    sink = BAGAdressenLandingWriter(table_name="bag_adressen")

    written = write_rows_to_sink(silver_rows, sink)
    typer.echo(f"Wrote {written} BAG landing rows")


@app.command("build-bag-landing-pand")
def build_bag_landing_pand(
    table_name: str = typer.Option(
        "bag_pand_flat",
        "--table",
        help="Silver table name to materialize into the Postgres landing layer.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Silver Iceberg namespace. Defaults to configured silver namespace.",
    ),
    run_id: str | None = typer.Option(
        None,
        help="Silver run identifier to materialize into the landing layer.",
    ),
) -> None:
    silver_rows = read_silver_rows(table_name=table_name, namespace=namespace, run_id=run_id)
    sink = BAGPandLandingWriter(table_name="bag_pand")

    written = write_rows_to_sink(silver_rows, sink)
    typer.echo(f"Wrote {written} BAG landing rows")
