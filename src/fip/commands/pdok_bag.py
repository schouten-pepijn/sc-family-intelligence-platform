from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import typer

from fip.cli import app
from fip.commands._helpers import dedupe_raw_records, read_bronze_rows, read_silver_rows
from fip.gold.core.service import write_rows_to_sink
from fip.gold.pdok_bag.bag_gpkg_layer_writer import BAGGpkgLayerLandingWriter
from fip.gold.pdok_bag.bag_gpkg_verblijfsobject_writer import (
    BAGGpkgVerblijfsobjectLandingWriter,
)
from fip.gold.source_runs_writer import SourceRunLandingWriter
from fip.ingestion.base import RawRecord
from fip.ingestion.pdok_bag.gpkg_cache import resolve_gpkg_source_ref
from fip.ingestion.pdok_bag.gpkg_source import GPKG_URL, PDOKBAGGeoPackageSource
from fip.lakehouse.bronze.bag_factory import BAGIcebergSinkFactory
from fip.lakehouse.silver.pdok_bag.bag_gpkg_layer_service import (
    write_bronze_rows_to_bag_gpkg_layer_sink,
)
from fip.lakehouse.silver.pdok_bag.bag_gpkg_layer_sink import BAGGpkgLayerSink
from fip.lakehouse.silver.pdok_bag.bag_gpkg_verblijfsobject_service import (
    write_bronze_rows_to_bag_gpkg_verblijfsobject_sink,
)
from fip.lakehouse.silver.pdok_bag.bag_gpkg_verblijfsobject_sink import (
    BAGGpkgVerblijfsobjectSink,
)
from fip.raw.manifest import LocalManifestWriter, S3ManifestWriter, SourceRunManifest
from fip.raw.reader import RawSnapshotReader, S3RawSnapshotReader
from fip.raw.writer import (
    RawSnapshotWriteHandle,
    RawSnapshotWriter,
    S3RawSnapshotWriter,
    serialize_raw_record,
)
from fip.settings import get_settings

GPKG_CACHE_DIR_OPTION = typer.Option(
    Path(".cache/pdok-bag"),
    help="Local cache directory for URL GeoPackage sources.",
)


def _bag_raw_reader(
    raw_target: str,
    raw_output_dir: Path,
) -> RawSnapshotReader | S3RawSnapshotReader:
    if raw_target == "local":
        return RawSnapshotReader(base_dir=raw_output_dir)
    if raw_target == "s3":
        return S3RawSnapshotReader()
    raise typer.BadParameter("raw_target must be either 'local' or 's3'")


@app.command("ingest-bag-gpkg")
def ingest_bag_gpkg(
    run_id: str = typer.Option(..., help="Run identifier for this ingestion."),
    layer: str = typer.Option(
        "verblijfsobject",
        help="BAG GeoPackage layer to ingest.",
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
    for record in reader.iter_bag_gpkg_records(run_id=run_id, layer=layer):
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


@app.command("archive-bag-gpkg")
def archive_bag_gpkg(
    run_id: str = typer.Option("debug-gpkg"),
    source_ref: str = typer.Option(
        GPKG_URL,
        help="GeoPackage path or URL.",
    ),
    layer: str = typer.Option(
        "verblijfsobject",
        help="BAG GeoPackage layer to archive.",
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
    cache_dir: Path = GPKG_CACHE_DIR_OPTION,
    refresh_cache: bool = typer.Option(
        False,
        help="Re-download URL GeoPackage sources even when a cached artifact exists.",
    ),
) -> None:
    started_at = datetime.now(timezone.utc)
    resolved_source_ref = resolve_gpkg_source_ref(
        source_ref=source_ref,
        cache_dir=cache_dir,
        refresh_cache=refresh_cache,
    )
    source = PDOKBAGGeoPackageSource(
        run_id=run_id,
        source_ref=resolved_source_ref,
        layer=layer,
        max_features=limit,
    )

    writer: RawSnapshotWriter | S3RawSnapshotWriter
    if target == "local":
        writer = RawSnapshotWriter(base_dir=str(output_dir))
    elif target == "s3":
        writer = S3RawSnapshotWriter()
    else:
        raise typer.BadParameter("target must be either 'local' or 's3'")

    status = "success"
    error_message: str | None = None
    archived = 0
    expected_entity: str | None = None
    handle: RawSnapshotWriteHandle | None = None
    written = 0

    try:
        for record in source.iter_records():
            if expected_entity is None:
                expected_entity = record.entity_name
                handle = writer.open_for_record(record)
            elif record.entity_name != expected_entity:
                raise ValueError("archive-bag-gpkg expects records for a single BAG layer")

            assert handle is not None
            handle.write(serialize_raw_record(record))
            handle.write("\n")
            archived += 1
        written = archived
    except Exception as exc:
        status = "failed"
        error_message = str(exc)
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        if handle is not None:
            handle.close()

        manifest = SourceRunManifest(
            source_name="bag_gpkg",
            source_family="pdok_bag",
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            source_url=str(source_ref),
            source_version=layer,
            license="pdok_open_data",
            attribution="PDOK / Kadaster BAG",
            raw_uri=(
                f"s3://{get_settings().s3_bucket}/raw/bag_gpkg/{run_id}/"
                if target == "s3"
                else str(output_dir / "raw" / "bag_gpkg" / run_id)
            ),
            row_count=archived,
            status=status,
            error_message=error_message,
        )

        if target == "local":
            LocalManifestWriter(base_dir=output_dir).write(manifest)
        elif target == "s3":
            S3ManifestWriter().write(manifest)

        SourceRunLandingWriter().write([manifest])

    typer.echo(f"Wrote {written} raw records")


@app.command("build-bag-gpkg-silver-verblijfsobject")
def build_bag_gpkg_silver_verblijfsobject(
    table_name: str = typer.Option(
        "bag_gpkg_verblijfsobject",
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
    sink = BAGGpkgVerblijfsobjectSink(
        table_ident=f"{silver_namespace}.bag_gpkg_verblijfsobject_flat"
    )

    written = write_bronze_rows_to_bag_gpkg_verblijfsobject_sink(bronze_rows, sink)
    typer.echo(f"Wrote {written} BAG GPKG Silver rows")


def _build_bag_gpkg_silver_layer(
    layer: str,
    table_name: str,
    namespace: str | None,
    run_id: str | None,
) -> None:
    bronze_rows = read_bronze_rows(table_name=table_name, namespace=namespace, run_id=run_id)
    silver_namespace = get_settings().silver_namespace
    sink = BAGGpkgLayerSink(
        layer=layer,
        table_ident=f"{silver_namespace}.bag_gpkg_{layer}_flat",
    )

    written = write_bronze_rows_to_bag_gpkg_layer_sink(layer, bronze_rows, sink)
    typer.echo(f"Wrote {written} BAG GPKG {layer} Silver rows")


@app.command("build-bag-gpkg-silver-pand")
def build_bag_gpkg_silver_pand(
    table_name: str = typer.Option(
        "bag_gpkg_pand",
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
    _build_bag_gpkg_silver_layer("pand", table_name, namespace, run_id)


@app.command("build-bag-gpkg-silver-woonplaats")
def build_bag_gpkg_silver_woonplaats(
    table_name: str = typer.Option(
        "bag_gpkg_woonplaats",
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
    _build_bag_gpkg_silver_layer("woonplaats", table_name, namespace, run_id)


@app.command("build-bag-gpkg-silver-ligplaats")
def build_bag_gpkg_silver_ligplaats(
    table_name: str = typer.Option(
        "bag_gpkg_ligplaats",
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
    _build_bag_gpkg_silver_layer("ligplaats", table_name, namespace, run_id)


@app.command("build-bag-gpkg-silver-standplaats")
def build_bag_gpkg_silver_standplaats(
    table_name: str = typer.Option(
        "bag_gpkg_standplaats",
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
    _build_bag_gpkg_silver_layer("standplaats", table_name, namespace, run_id)


@app.command("build-bag-gpkg-landing-verblijfsobject")
def build_bag_gpkg_landing_verblijfsobject(
    table_name: str = typer.Option(
        "bag_gpkg_verblijfsobject_flat",
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
    sink = BAGGpkgVerblijfsobjectLandingWriter(table_name="bag_gpkg_verblijfsobject")

    written = write_rows_to_sink(silver_rows, sink)
    typer.echo(f"Wrote {written} BAG GPKG landing rows")


def _build_bag_gpkg_landing_layer(
    layer: str,
    table_name: str,
    namespace: str | None,
    run_id: str | None,
) -> None:
    silver_rows = read_silver_rows(table_name=table_name, namespace=namespace, run_id=run_id)
    sink = BAGGpkgLayerLandingWriter(layer=layer, table_name=f"bag_gpkg_{layer}")

    written = write_rows_to_sink(silver_rows, sink)
    typer.echo(f"Wrote {written} BAG GPKG {layer} landing rows")


@app.command("build-bag-gpkg-landing-pand")
def build_bag_gpkg_landing_pand(
    table_name: str = typer.Option(
        "bag_gpkg_pand_flat",
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
    _build_bag_gpkg_landing_layer("pand", table_name, namespace, run_id)


@app.command("build-bag-gpkg-landing-woonplaats")
def build_bag_gpkg_landing_woonplaats(
    table_name: str = typer.Option(
        "bag_gpkg_woonplaats_flat",
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
    _build_bag_gpkg_landing_layer("woonplaats", table_name, namespace, run_id)


@app.command("build-bag-gpkg-landing-ligplaats")
def build_bag_gpkg_landing_ligplaats(
    table_name: str = typer.Option(
        "bag_gpkg_ligplaats_flat",
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
    _build_bag_gpkg_landing_layer("ligplaats", table_name, namespace, run_id)


@app.command("build-bag-gpkg-landing-standplaats")
def build_bag_gpkg_landing_standplaats(
    table_name: str = typer.Option(
        "bag_gpkg_standplaats_flat",
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
    _build_bag_gpkg_landing_layer("standplaats", table_name, namespace, run_id)
