from __future__ import annotations

from pathlib import Path

import typer

from fip.cli import app
from fip.commands._helpers import (
    build_gold_reference_codes,
    dedupe_raw_records,
    read_bronze_rows,
    read_silver_rows,
)
from fip.gold.cbs.cbs_observations_writer import CBSObservationLandingWriter
from fip.gold.core.service import write_rows_to_sink
from fip.ingestion.base import RawRecord
from fip.ingestion.cbs.adapter import CBSODataSource
from fip.lakehouse.bronze.cbs_factory import CBSIcebergSinkFactory
from fip.lakehouse.silver.cbs.cbs_observations_service import (
    write_bronze_rows_to_cbs_observation_sink,
)
from fip.lakehouse.silver.cbs.cbs_observations_sink import CBSObservationSink
from fip.raw.reader import MinioRawSnapshotReader, RawSnapshotReader
from fip.raw.writer import MinioRawSnapshotWriter, RawSnapshotWriter
from fip.settings import get_settings


@app.command("ingest-cbs")
def ingest_cbs(
    table_id: str = typer.Option(..., help="CBS table id, for example 83625NED."),
    run_id: str = typer.Option(..., help="Run identifier for this ingestion."),
    target_namespace: str | None = typer.Option(None, help="Target Iceberg namespace."),
    limit: int = typer.Option(1000, min=1, help="Maximum number of CBS records to ingest."),
    raw_target: str = typer.Option("local", help="Raw source: local JSONL files or MinIO."),
    raw_output_dir: Path = Path(".raw"),
) -> None:
    if target_namespace is None:
        target_namespace = get_settings().bronze_namespace

    reader: RawSnapshotReader | MinioRawSnapshotReader
    if raw_target == "local":
        reader = RawSnapshotReader(base_dir=raw_output_dir)
    elif raw_target == "minio":
        reader = MinioRawSnapshotReader()
    else:
        raise typer.BadParameter("raw_target must be either 'local' or 'minio'")

    sink_factory = CBSIcebergSinkFactory(namespace=target_namespace)
    grouped_records: dict[str, list[RawRecord]] = {}
    seen = 0

    for record in reader.iter_cbs_records(table_id=table_id, run_id=run_id):
        grouped_records.setdefault(record.entity_name, []).append(record)
        seen += 1
        if seen >= limit:
            break

    written = 0
    for entity_name, records in grouped_records.items():
        sink = sink_factory.for_entity(entity_name)
        deduped_records = dedupe_raw_records(records)
        written += sink.write(deduped_records)

    typer.echo(f"Wrote {written} records using sink namespace {target_namespace}")


@app.command("archive-cbs-raw")
def archive_cbs_raw(
    table_id: str = typer.Option("83625NED"),
    run_id: str = typer.Option("debug-raw"),
    limit: int | None = typer.Option(
        None,
        help="Maximum number of raw records to archive. Leave unset for a full pull.",
    ),
    target: str = typer.Option(
        "local",
        help="Raw storage target: local JSONL files or MinIO object storage.",
    ),
    output_dir: Path = Path(".raw"),
) -> None:
    source = CBSODataSource(table_id=table_id, run_id=run_id)

    writer: RawSnapshotWriter | MinioRawSnapshotWriter
    if target == "local":
        writer = RawSnapshotWriter(base_dir=str(output_dir))
    elif target == "minio":
        writer = MinioRawSnapshotWriter()
    else:
        raise typer.BadParameter("target must be either 'local' or 'minio'")

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

    typer.echo(f"Wrote {written} raw records")


@app.command("build-cbs-silver-observations")
def build_cbs_silver_observations(
    table_name: str = typer.Option(
        "cbs_observations_83625ned",
        "--table",
        help="Bronze table name to transform into Silver.",
    ),
    silver_table_name: str = typer.Option(
        "cbs_observations_flat_83625ned",
        "--silver-table",
        help="Silver table name to write to.",
    ),
    run_id: str = typer.Option(
        "debug-raw",
        help="Bronze run identifier to materialize into Silver.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Bronze Iceberg namespace. Defaults to configured bronze namespace.",
    ),
) -> None:
    bronze_rows = read_bronze_rows(table_name=table_name, namespace=namespace, run_id=run_id)
    silver_namespace = get_settings().silver_namespace
    sink = CBSObservationSink(
        table_ident=f"{silver_namespace}.{silver_table_name}",
    )

    written = write_bronze_rows_to_cbs_observation_sink(bronze_rows, sink)
    typer.echo(f"Wrote {written} Silver rows")


@app.command("build-gold-measure-codes")
def build_gold_measure_codes(
    table_id: str = typer.Option("83625NED", help="CBS table id to ingest."),
    run_id: str = typer.Option("debug-raw", help="Run identifier for this export."),
    raw_target: str = typer.Option("local", help="Raw source: local JSONL files or MinIO."),
    raw_output_dir: Path = Path(".raw"),
) -> None:
    written = build_gold_reference_codes(
        table_id=table_id,
        run_id=run_id,
        entity="MeasureCodes",
        table_name="cbs_measure_codes",
        raw_target=raw_target,
        raw_output_dir=raw_output_dir,
    )
    typer.echo(f"Wrote {written} MeasureCodes rows into cbs_measure_codes")


@app.command("build-gold-period-codes")
def build_gold_period_codes(
    table_id: str = typer.Option("83625NED", help="CBS table id to ingest."),
    run_id: str = typer.Option("debug-raw", help="Run identifier for this export."),
    raw_target: str = typer.Option("local", help="Raw source: local JSONL files or MinIO."),
    raw_output_dir: Path = Path(".raw"),
) -> None:
    written = build_gold_reference_codes(
        table_id=table_id,
        run_id=run_id,
        entity="PeriodenCodes",
        table_name="cbs_period_codes",
        raw_target=raw_target,
        raw_output_dir=raw_output_dir,
    )
    typer.echo(f"Wrote {written} PeriodenCodes rows into cbs_period_codes")


@app.command("build-gold-region-codes")
def build_gold_region_codes(
    table_id: str = typer.Option("83625NED", help="CBS table id to ingest."),
    run_id: str = typer.Option("debug-raw", help="Run identifier for this export."),
    raw_target: str = typer.Option("local", help="Raw source: local JSONL files or MinIO."),
    raw_output_dir: Path = Path(".raw"),
) -> None:
    written = build_gold_reference_codes(
        table_id=table_id,
        run_id=run_id,
        entity="RegioSCodes",
        table_name="cbs_region_codes",
        raw_target=raw_target,
        raw_output_dir=raw_output_dir,
    )
    typer.echo(f"Wrote {written} RegioSCodes rows into cbs_region_codes")


@app.command("build-gold-eigendom-codes")
def build_gold_eigendom_codes(
    table_id: str = typer.Option("85036NED", help="CBS table id to ingest."),
    run_id: str = typer.Option("debug-raw", help="Run identifier for this export."),
    raw_target: str = typer.Option("local", help="Raw source: local JSONL files or MinIO."),
    raw_output_dir: Path = Path(".raw"),
) -> None:
    written = build_gold_reference_codes(
        table_id=table_id,
        run_id=run_id,
        entity="EigendomCodes",
        table_name="cbs_eigendom_codes",
        raw_target=raw_target,
        raw_output_dir=raw_output_dir,
    )
    typer.echo(f"Wrote {written} EigendomCodes rows into cbs_eigendom_codes")


@app.command("build-landing-observations")
def build_landing_observations(
    table_name: str = typer.Option(
        "cbs_observations_flat_83625ned",
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
    sink = CBSObservationLandingWriter(table_name="cbs_observations")

    written = write_rows_to_sink(silver_rows, sink)
    typer.echo(f"Wrote {written} landing rows")
