from __future__ import annotations

from pathlib import Path

import fip.cli as cli
from fip.ingestion.base import RawRecord


@cli.app.command("ingest-bag")
def ingest_bag(
    run_id: str = cli.typer.Option(..., help="Run identifier for this ingestion."),
    collection: str = cli.typer.Option(
        "verblijfsobject",
        help="BAG collection to ingest, for example verblijfsobject or pand.",
    ),
    target_namespace: str | None = cli.typer.Option(
        None,
        help="Target Iceberg namespace.",
    ),
    limit: int = cli.typer.Option(
        1000,
        min=1,
        help="Maximum number of BAG records to ingest.",
    ),
    progress_every: int = cli.typer.Option(
        1000,
        min=1,
        help="Print progress every N records while reading BAG.",
    ),
) -> None:
    if target_namespace is None:
        target_namespace = cli.get_settings().bronze_namespace

    source = cli.PDOKBAGSource(run_id=run_id, collection=collection)
    sink_factory = cli.BAGIcebergSinkFactory(namespace=target_namespace)

    grouped_records: dict[str, list[RawRecord]] = {}
    seen = 0
    for record in source.iter_records():
        grouped_records.setdefault(record.entity_name, []).append(record)
        seen += 1
        if seen % progress_every == 0:
            cli.typer.echo(f"Read {seen} BAG records...")
        if seen >= limit:
            break

    written = 0
    for entity_name, records in grouped_records.items():
        sink = sink_factory.for_entity(entity_name)
        written += sink.write(records)

    cli.typer.echo(f"Wrote {written} records using sink namespace {target_namespace}")


@cli.app.command("archive-bag-raw")
def archive_bag_raw(
    run_id: str = cli.typer.Option("debug-raw"),
    collection: str = cli.typer.Option(
        "verblijfsobject",
        help="BAG collection to archive, for example verblijfsobject or pand.",
    ),
    limit: int | None = cli.typer.Option(
        None,
        help="Maximum number of raw records to archive. Leave unset for a full pull.",
    ),
    target: str = cli.typer.Option("local", help="Raw storage target: local JSONL files or MinIO."),
    output_dir: Path = Path(".raw"),
) -> None:
    source = cli.PDOKBAGSource(run_id=run_id, collection=collection)

    writer: cli.RawSnapshotWriter | cli.MinioRawSnapshotWriter
    if target == "local":
        writer = cli.RawSnapshotWriter(base_dir=str(output_dir))
    elif target == "minio":
        writer = cli.MinioRawSnapshotWriter()
    else:
        raise cli.typer.BadParameter("target must be either 'local' or 'minio'")

    grouped: dict[str, list[RawRecord]] = {}
    archived = 0
    for record in source.iter_records():
        grouped.setdefault(record.entity_name, []).append(record)
        archived += 1
        if limit is not None and archived >= limit:
            break

    written = 0
    for records in grouped.values():
        written += writer.write(records)

    cli.typer.echo(f"Wrote {written} raw records")


@cli.app.command("build-bag-silver-verblijfsobject")
def build_bag_silver_verblijfsobject(
    table_name: str = cli.typer.Option(
        "bag_verblijfsobject",
        "--table",
        help="Bronze table name to transform into Silver.",
    ),
    namespace: str | None = cli.typer.Option(
        None,
        help="Bronze Iceberg namespace. Defaults to configured bronze namespace.",
    ),
) -> None:
    bronze_rows = cli._read_bronze_rows(table_name=table_name, namespace=namespace)
    silver_namespace = cli.get_settings().silver_namespace
    sink = cli.BAGVerblijfsobjectSink(
        table_ident=f"{silver_namespace}.bag_verblijfsobject_flat",
    )

    written = cli.write_bronze_rows_to_bag_verblijfsobject_sink(bronze_rows, sink)
    cli.typer.echo(f"Wrote {written} BAG Silver rows")


@cli.app.command("build-bag-silver-pand")
def build_bag_silver_pand(
    table_name: str = cli.typer.Option(
        "bag_pand",
        "--table",
        help="Bronze table name to transform into Silver.",
    ),
    namespace: str | None = cli.typer.Option(
        None,
        help="Bronze Iceberg namespace. Defaults to configured bronze namespace.",
    ),
) -> None:
    bronze_rows = cli._read_bronze_rows(table_name=table_name, namespace=namespace)
    silver_namespace = cli.get_settings().silver_namespace
    sink = cli.BAGPandSink(
        table_ident=f"{silver_namespace}.bag_pand_flat",
    )

    written = cli.write_bronze_rows_to_bag_pand_sink(bronze_rows, sink)
    cli.typer.echo(f"Wrote {written} BAG Silver rows")


@cli.app.command("build-bag-landing-verblijfsobject")
def build_bag_landing_verblijfsobject(
    table_name: str = cli.typer.Option(
        "bag_verblijfsobject_flat",
        "--table",
        help="Silver table name to materialize into the Postgres landing layer.",
    ),
    namespace: str | None = cli.typer.Option(
        None,
        help="Silver Iceberg namespace. Defaults to configured silver namespace.",
    ),
) -> None:
    silver_rows = cli._read_silver_rows(table_name=table_name, namespace=namespace)
    sink = cli.BAGVerblijfsobjectLandingWriter(table_name="bag_verblijfsobject")

    written = cli.write_rows_to_sink(silver_rows, sink)
    cli.typer.echo(f"Wrote {written} BAG landing rows")


@cli.app.command("build-bag-landing-pand")
def build_bag_landing_pand(
    table_name: str = cli.typer.Option(
        "bag_pand_flat",
        "--table",
        help="Silver table name to materialize into the Postgres landing layer.",
    ),
    namespace: str | None = cli.typer.Option(
        None,
        help="Silver Iceberg namespace. Defaults to configured silver namespace.",
    ),
) -> None:
    silver_rows = cli._read_silver_rows(table_name=table_name, namespace=namespace)
    sink = cli.BAGPandLandingWriter(table_name="bag_pand")

    written = cli.write_rows_to_sink(silver_rows, sink)
    cli.typer.echo(f"Wrote {written} BAG landing rows")
