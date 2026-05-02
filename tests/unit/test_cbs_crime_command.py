from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from typer.testing import CliRunner

from fip import cli
from fip.ingestion.base import RawRecord

runner = CliRunner()


def test_archive_cbs_crime_raw_command_writes_records(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        name = "cbs_crime"
        table_id = "83648NED"
        base_url = "https://datasets.cbs.nl/odata/v1/CBS/83648NED"

        def __init__(self, run_id: str) -> None:
            calls["run_id"] = run_id

        def iter_records(self):
            yield RawRecord(
                source_name="cbs_crime",
                entity_name="83648NED.Observations",
                natural_key="1",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="crime-run",
                payload={"Id": 1, "Measure": "M004200_2"},
                schema_version="v1",
                http_status=200,
            )
            yield RawRecord(
                source_name="cbs_crime",
                entity_name="83648NED.SoortMisdrijfCodes",
                natural_key="1.1",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="crime-run",
                payload={"Id": "1.1", "Title": "Misdrijven totaal"},
                schema_version="v1",
                http_status=200,
            )

    class FakeLocalWriter:
        def __init__(self, base_dir: str) -> None:
            calls["writer"] = "local"
            calls["base_dir"] = base_dir

        def write(self, records: list[RawRecord]) -> int:
            batches = cast(list[list[RawRecord]], calls.setdefault("batches", []))
            batches.append(records)
            return len(records)

    class FakeS3Writer:
        def __init__(self) -> None:
            calls["writer"] = "s3"

        def write(self, records: list[RawRecord]) -> int:
            batches = cast(list[list[RawRecord]], calls.setdefault("batches", []))
            batches.append(records)
            return len(records)

    class FakeLocalManifestWriter:
        def __init__(self, base_dir: str) -> None:
            calls["manifest_writer"] = "local"
            calls["manifest_base_dir"] = base_dir

        def write(self, manifest, table_id=None):
            calls["manifest"] = manifest
            calls["manifest_table_id"] = table_id
            return None

    class FakeS3ManifestWriter:
        def __init__(self) -> None:
            calls["manifest_writer"] = "s3"

        def write(self, manifest, table_id=None):
            calls["manifest"] = manifest
            calls["manifest_table_id"] = table_id
            return None

    class FakeSourceRunLandingWriter:
        def write(self, manifests) -> int:
            rows = list(manifests)
            calls["source_run_writer"] = "called"
            calls["source_run_manifests"] = rows
            return len(rows)

    monkeypatch.setattr("fip.commands.cbs_crime.CBSCrimeSource", FakeSource)
    monkeypatch.setattr("fip.commands.cbs_crime.RawSnapshotWriter", FakeLocalWriter)
    monkeypatch.setattr("fip.commands.cbs_crime.S3RawSnapshotWriter", FakeS3Writer)
    monkeypatch.setattr("fip.commands.cbs_crime.LocalManifestWriter", FakeLocalManifestWriter)
    monkeypatch.setattr("fip.commands.cbs_crime.S3ManifestWriter", FakeS3ManifestWriter)
    monkeypatch.setattr("fip.commands.cbs_crime.SourceRunLandingWriter", FakeSourceRunLandingWriter)

    result = runner.invoke(
        cli.app,
        [
            "archive-cbs-crime-raw",
            "--run-id",
            "crime-run",
            "--target",
            "local",
            "--output-dir",
            ".raw-smoke",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 2 raw records\n"
    assert calls["run_id"] == "crime-run"
    assert calls["writer"] == "local"
    assert calls["base_dir"] == ".raw-smoke"
    batches = cast(list[list[RawRecord]], calls["batches"])
    assert [[record.entity_name for record in batch] for batch in batches] == [
        ["83648NED.Observations"],
        ["83648NED.SoortMisdrijfCodes"],
    ]
    assert calls["manifest_writer"] == "local"
    assert calls["manifest_table_id"] == "83648NED"
    assert calls["source_run_writer"] == "called"
    assert len(calls["source_run_manifests"]) == 1
    manifest = calls["manifest"]
    assert manifest.source_name == "cbs_crime"
    assert manifest.source_family == "cbs"
    assert manifest.run_id == "crime-run"
    assert manifest.source_version == "83648NED"
    assert manifest.row_count == 2
    assert manifest.status == "success"
    assert manifest.error_message is None
    assert manifest.source_url == "https://datasets.cbs.nl/odata/v1/CBS/83648NED"
    assert manifest.license == "cbs_open_data"
    assert manifest.attribution == "CBS StatLine"
    assert Path(manifest.raw_uri) == Path(".raw-smoke") / "raw" / "cbs" / "83648NED" / "crime-run"
