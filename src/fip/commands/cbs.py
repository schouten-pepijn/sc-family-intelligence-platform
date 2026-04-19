from __future__ import annotations

from pathlib import Path

import fip.cli as cli
from fip.ingestion.base import RawRecord


@cli.app.command("ingest-cbs")
def ingest_cbs(
    table_id: str = cli.typer.Option(..., help="CBS table id, for example 83625NED."),
    run_id: str = cli.typer.Option(..., help="Run identifier for this ingestion."),
    target_namespace: str | None = cli.typer.Option(
        None,
        help="Target Iceberg namespace.",
    ),
    limit: int = cli.typer.Option(
        1000,
        min=1,
        help="Maximum number of CBS records to ingest.",
    ),
) -> None:
    if target_namespace is None:
        target_namespace = cli.get_settings().bronze_namespace

    source = cli.CBSODataSource(table_id=table_id, run_id=run_id)
    sink_factory = cli.CBSIcebergSinkFactory(namespace=target_namespace)

    grouped_records: dict[str, list[RawRecord]] = {}
    seen = 0
    for record in source.iter_records():
        grouped_records.setdefault(record.entity_name, []).append(record)
        seen += 1
        if seen >= limit:
            break

    written = 0
    for entity_name, records in grouped_records.items():
        sink = sink_factory.for_entity(entity_name)
        written += sink.write(records)

    cli.typer.echo(f"Wrote {written} records using sink namespace {target_namespace}")


@cli.app.command("archive-cbs-raw")
def archive_cbs_raw(
    table_id: str = cli.typer.Option("83625NED"),
    run_id: str = cli.typer.Option("debug-raw"),
    limit: int | None = cli.typer.Option(
        None,
        help="Maximum number of raw records to archive. Leave unset for a full pull.",
    ),
    target: str = cli.typer.Option(
        "local",
        help="Raw storage target: local JSONL files or MinIO object storage.",
    ),
    output_dir: Path = Path(".raw"),
) -> None:
    source = cli.CBSODataSource(table_id=table_id, run_id=run_id)

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


@cli.app.command("build-cbs-silver-observations")
def build_cbs_silver_observations(
    table_name: str = cli.typer.Option(
        "cbs_observations_83625ned",
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
    sink = cli.CBSObservationSink(
        table_ident=f"{silver_namespace}.cbs_observations_flat_83625ned",
    )

    written = cli.write_bronze_rows_to_cbs_observation_sink(bronze_rows, sink)
    cli.typer.echo(f"Wrote {written} Silver rows")


@cli.app.command("build-gold-measure-codes")
def build_gold_measure_codes(
    table_id: str = cli.typer.Option("83625NED", help="CBS table id to ingest."),
    run_id: str = cli.typer.Option("debug-raw", help="Run identifier for this export."),
) -> None:
    cli._build_gold_reference_codes(
        table_id=table_id,
        run_id=run_id,
        entity="MeasureCodes",
        table_name="cbs_measure_codes",
    )


@cli.app.command("build-gold-period-codes")
def build_gold_period_codes(
    table_id: str = cli.typer.Option("83625NED", help="CBS table id to ingest."),
    run_id: str = cli.typer.Option("debug-raw", help="Run identifier for this export."),
) -> None:
    cli._build_gold_reference_codes(
        table_id=table_id,
        run_id=run_id,
        entity="PeriodenCodes",
        table_name="cbs_period_codes",
    )


@cli.app.command("build-gold-region-codes")
def build_gold_region_codes(
    table_id: str = cli.typer.Option("83625NED", help="CBS table id to ingest."),
    run_id: str = cli.typer.Option("debug-raw", help="Run identifier for this export."),
) -> None:
    cli._build_gold_reference_codes(
        table_id=table_id,
        run_id=run_id,
        entity="RegioSCodes",
        table_name="cbs_region_codes",
    )


@cli.app.command("build-landing-observations")
def build_landing_observations(
    table_name: str = cli.typer.Option(
        "cbs_observations_flat_83625ned",
        "--table",
        help="Silver table name to materialize into the Postgres landing layer.",
    ),
    namespace: str | None = cli.typer.Option(
        None,
        help="Silver Iceberg namespace. Defaults to configured silver namespace.",
    ),
) -> None:
    silver_rows = cli._read_silver_rows(table_name=table_name, namespace=namespace)
    sink = cli.CBSObservationLandingWriter(table_name="cbs_observations")

    written = cli.write_rows_to_sink(silver_rows, sink)
    cli.typer.echo(f"Wrote {written} landing rows")
