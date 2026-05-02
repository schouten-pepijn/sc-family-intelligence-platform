from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import typer

from fip.cli import app
from fip.commands._helpers import read_bronze_rows
from fip.gold.source_runs_writer import SourceRunLandingWriter
from fip.ingestion.base import RawRecord
from fip.ingestion.cbs_crime.adapter import CBSCrimeSource
from fip.lakehouse.silver.cbs_crime.cbs_crime_observations_service import (
    write_bronze_rows_to_cbs_crime_observation_sink,
)
from fip.lakehouse.silver.cbs_crime.cbs_crime_observations_sink import (
    CBSCrimeObservationSink,
)
from fip.raw.manifest import LocalManifestWriter, S3ManifestWriter, SourceRunManifest
from fip.raw.writer import RawSnapshotWriter, S3RawSnapshotWriter
from fip.settings import get_settings


@app.command("archive-cbs-crime-raw")
def archive_cbs_crime_raw(
    run_id: str = typer.Option("debug-raw"),
    target: str = typer.Option("s3"),
    output_dir: Path = Path(".raw"),
) -> None:
    started_at = datetime.now(timezone.utc)
    source = CBSCrimeSource(run_id=run_id)

    writer: RawSnapshotWriter | S3RawSnapshotWriter
    if target == "local":
        writer = RawSnapshotWriter(base_dir=str(output_dir))
    elif target == "s3":
        writer = S3RawSnapshotWriter()
    else:
        raise typer.BadParameter("target must be either 'local' or 's3'")

    status = "success"
    error_message: str | None = None
    grouped: dict[str, list[RawRecord]] = {}
    archived = 0
    written = 0
    try:
        for record in source.iter_records():
            grouped.setdefault(record.entity_name, []).append(record)
            archived += 1
        for records in grouped.values():
            written += writer.write(records)
    except Exception as exc:
        status = "failed"
        error_message = str(exc)
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        manifest = SourceRunManifest(
            source_name=source.name,
            source_family="cbs",
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            source_url=source.base_url,
            source_version=source.table_id,
            license="cbs_open_data",
            attribution="CBS StatLine",
            raw_uri=(
                f"s3://{get_settings().s3_bucket}/raw/cbs/{source.table_id}/{run_id}/"
                if target == "s3"
                else str(output_dir / "raw" / "cbs" / source.table_id / run_id)
            ),
            row_count=archived,
            status=status,
            error_message=error_message,
        )

        if target == "local":
            LocalManifestWriter(base_dir=output_dir).write(manifest, table_id=source.table_id)
        else:
            S3ManifestWriter().write(manifest, table_id=source.table_id)

        SourceRunLandingWriter().write([manifest])

    typer.echo(f"Wrote {written} raw records")


@app.command("build-cbs-crime-silver-observations")
def build_cbs_crime_silver_observations(
    table_name: str = typer.Option(
        "cbs_observations_83648ned",
        "--table",
        help="Bronze CBS crime observations table name to transform into Silver.",
    ),
    silver_table_name: str = typer.Option(
        "cbs_crime_observations_flat_83648ned",
        "--silver-table",
        help="Silver CBS crime observations table name to write to.",
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
    sink = CBSCrimeObservationSink(
        table_ident=f"{silver_namespace}.{silver_table_name}",
    )

    written = write_bronze_rows_to_cbs_crime_observation_sink(bronze_rows, sink)
    typer.echo(f"Wrote {written} Silver crime rows")
