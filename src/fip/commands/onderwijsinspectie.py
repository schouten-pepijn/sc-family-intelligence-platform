from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import typer

from fip.cli import app
from fip.gold.source_runs_writer import SourceRunLandingWriter
from fip.ingestion.onderwijsinspectie.adapter import OnderwijsInspectieSource
from fip.raw.manifest import LocalManifestWriter, S3ManifestWriter, SourceRunManifest
from fip.raw.writer import RawSnapshotWriter, S3RawSnapshotWriter, serialize_raw_record
from fip.settings import get_settings


@app.command("archive-onderwijsinspectie-raw")
def archive_onderwijsinspectie_raw(
    source_ref: str = typer.Option(
        ..., help="Path, page URL, or direct ODS download for the official dataset."
    ),
    run_id: str = typer.Option("debug-raw"),
    target: str = typer.Option("s3"),
    output_dir: Path = Path(".raw"),
) -> None:
    started_at = datetime.now(timezone.utc)
    source = OnderwijsInspectieSource(source_ref=source_ref, run_id=run_id)

    writer: RawSnapshotWriter | S3RawSnapshotWriter
    if target == "local":
        writer = RawSnapshotWriter(base_dir=str(output_dir))
    elif target == "s3":
        writer = S3RawSnapshotWriter()
    else:
        raise typer.BadParameter("target must be either 'local' or 's3'")

    archived = 0
    handle = None
    try:
        for record in source.iter_records():
            if handle is None:
                handle = writer.open_for_record(record)
            handle.write(serialize_raw_record(record))
            handle.write("\n")
            archived += 1
    finally:
        finished_at = datetime.now(timezone.utc)
        if handle is not None:
            handle.close()

        manifest = SourceRunManifest(
            source_name=source.name,
            source_family="inspection",
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            source_url=source_ref,
            source_version=source.schema_version,
            license="unknown",
            attribution="Inspectie van het Onderwijs",
            raw_uri=(
                f"s3://{get_settings().s3_bucket}/raw/inspection/{run_id}/"
                if target == "s3"
                else str(output_dir / "raw" / "inspection" / run_id)
            ),
            row_count=archived,
            status="success",
            error_message=None,
        )

        if target == "local":
            LocalManifestWriter(base_dir=output_dir).write(manifest)
        else:
            S3ManifestWriter().write(manifest)

        SourceRunLandingWriter().write([manifest])

    typer.echo(f"Wrote {archived} raw records")
