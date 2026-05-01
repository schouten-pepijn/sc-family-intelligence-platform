from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import pytest
from typer.testing import CliRunner

from fip import cli
from fip.ingestion.base import RawRecord

runner = CliRunner()


def test_ingest_cbs_command_invokes_service_and_prints_result(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeReader:
        def __init__(self, base_dir) -> None:
            calls["base_dir"] = base_dir

        def iter_cbs_records(self, table_id: str, run_id: str):
            calls["table_id"] = table_id
            calls["run_id"] = run_id

            yield RawRecord(
                source_name="cbs_statline",
                entity_name="83625NED.MeasureCodes",
                natural_key="M001534",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="run-001",
                payload={"Identifier": "M001534"},
                schema_version="v1",
            )
            yield RawRecord(
                source_name="cbs_statline",
                entity_name="83625NED.MeasureCodes",
                natural_key="M001535",
                retrieved_at=datetime(2026, 4, 18, 9, 1, tzinfo=timezone.utc),
                run_id="run-001",
                payload={"Identifier": "M001535"},
                schema_version="v1",
            )

    class FakeSinkFactory:
        def __init__(self, namespace: str) -> None:
            calls["target_namespace"] = namespace

        def for_entity(self, entity_name: str):
            calls["entity_name"] = entity_name

            class FakeSink:
                def write(self, records) -> int:
                    rows = list(records)
                    calls["rows"] = rows
                    return len(rows)

            return FakeSink()

    monkeypatch.setattr("fip.commands.cbs.RawSnapshotReader", FakeReader)
    monkeypatch.setattr("fip.commands.cbs.S3RawSnapshotReader", FakeReader)
    monkeypatch.setattr("fip.commands.cbs.CBSIcebergSinkFactory", FakeSinkFactory)

    result = runner.invoke(
        cli.app,
        [
            "ingest-cbs",
            "--table-id",
            "83625NED",
            "--run-id",
            "run-001",
            "--target-namespace",
            "bronze",
            "--limit",
            "1",
            "--raw-target",
            "local",
            "--raw-output-dir",
            ".raw-smoke",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 records using sink namespace bronze\n"
    assert calls["table_id"] == "83625NED"
    assert calls["run_id"] == "run-001"
    assert calls["target_namespace"] == "bronze"
    assert calls["entity_name"] == "83625NED.MeasureCodes"
    rows = cast(list[RawRecord], calls["rows"])
    assert len(rows) == 1
    assert calls["base_dir"] == Path(".raw-smoke")


def test_ingest_bag_gpkg_command_invokes_service_and_prints_result(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeReader:
        def __init__(self, base_dir) -> None:
            calls["base_dir"] = base_dir

        def iter_bag_gpkg_records(self, run_id: str, layer: str):
            calls["run_id"] = run_id
            calls["layer"] = layer
            yield RawRecord(
                source_name="bag_gpkg",
                entity_name="bag_gpkg.verblijfsobject",
                natural_key="0003010000126809",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="run-003",
                payload={
                    "feature_id": 1,
                    "identificatie": "0003010000126809",
                    "geometry": {"type": "Point", "coordinates": [12345, 456789]},
                },
                schema_version="v1",
            )

    class FakeSinkFactory:
        def __init__(self, namespace: str) -> None:
            calls["target_namespace"] = namespace

        def for_entity(self, entity_name: str):
            calls["entity_name"] = entity_name

            class FakeSink:
                def write(self, records) -> int:
                    rows = list(records)
                    calls["rows"] = rows
                    return len(rows)

            return FakeSink()

    monkeypatch.setattr("fip.commands.pdok_bag.RawSnapshotReader", FakeReader)
    monkeypatch.setattr("fip.commands.pdok_bag.S3RawSnapshotReader", FakeReader)
    monkeypatch.setattr("fip.commands.pdok_bag.BAGIcebergSinkFactory", FakeSinkFactory)

    result = runner.invoke(
        cli.app,
        [
            "ingest-bag-gpkg",
            "--run-id",
            "run-003",
            "--layer",
            "verblijfsobject",
            "--target-namespace",
            "bronze",
            "--limit",
            "1",
            "--progress-every",
            "1",
            "--raw-target",
            "local",
            "--raw-output-dir",
            ".raw-smoke",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Read 1 BAG records...\nWrote 1 records using sink namespace bronze\n"
    assert calls["run_id"] == "run-003"
    assert calls["layer"] == "verblijfsobject"
    assert calls["target_namespace"] == "bronze"
    assert calls["entity_name"] == "bag_gpkg.verblijfsobject"
    rows = cast(list[RawRecord], calls["rows"])
    assert len(rows) == 1
    assert calls["base_dir"] == Path(".raw-smoke")


def test_inspect_cbs_raw_command_prints_filtered_payloads(monkeypatch) -> None:
    class FakeSource:
        def __init__(self, table_id: str, run_id: str) -> None:
            self.table_id = table_id
            self.run_id = run_id

        def iter_records(self):
            yield RawRecord(
                source_name="cbs_statline",
                entity_name="83625NED.Observations",
                natural_key="1",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"Id": 1, "Measure": "M001534"},
                schema_version="v1",
            )
            yield RawRecord(
                source_name="cbs_statline",
                entity_name="83625NED.MeasureCodes",
                natural_key="10",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"Id": 10, "Title": "Measure A"},
                schema_version="v1",
            )

    monkeypatch.setattr("fip.commands.inspection.CBSODataSource", FakeSource)

    result = runner.invoke(
        cli.app,
        [
            "inspect-cbs-raw",
            "--entity",
            "MeasureCodes",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == (
        '83625NED.MeasureCodes\nnatural_key=10\n{\n  "Id": 10,\n  "Title": "Measure A"\n}\n\n'
    )


@pytest.mark.parametrize(
    ("target", "expected_writer"),
    [
        ("local", "local"),
        ("s3", "s3"),
    ],
)
def test_archive_cbs_raw_command_selects_target_writer(
    monkeypatch, target: str, expected_writer: str
) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        def __init__(self, table_id: str, run_id: str) -> None:
            calls["table_id"] = table_id
            calls["run_id"] = run_id

        def iter_records(self):
            yield RawRecord(
                source_name="cbs_statline",
                entity_name="83625NED.MeasureCodes",
                natural_key="M001534",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"Identifier": "M001534", "Title": "Measure A"},
                schema_version="v1",
                http_status=200,
            )

    class FakeLocalWriter:
        def __init__(self, base_dir: str) -> None:
            calls["writer"] = "local"
            calls["base_dir"] = base_dir

        def write(self, records) -> int:
            rows = list(records)
            calls["rows"] = rows
            return len(rows)

        def open_for_record(self, record: RawRecord):
            calls["rows"] = []

            class FakeHandle:
                def write(self, value: str) -> None:
                    if value != "\n":
                        cast(list[str], calls["rows"]).append(value)

                def close(self) -> None:
                    return None

            return FakeHandle()

    class FakeS3Writer:
        def __init__(self) -> None:
            calls["writer"] = "s3"

        def write(self, records) -> int:
            rows = list(records)
            calls["rows"] = rows
            return len(rows)

        def open_for_record(self, record: RawRecord):
            calls["rows"] = []

            class FakeHandle:
                def write(self, value: str) -> None:
                    if value != "\n":
                        cast(list[str], calls["rows"]).append(value)

                def close(self) -> None:
                    return None

            return FakeHandle()

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

    monkeypatch.setattr("fip.commands.cbs.CBSODataSource", FakeSource)
    monkeypatch.setattr("fip.commands.cbs.RawSnapshotWriter", FakeLocalWriter)
    monkeypatch.setattr("fip.commands.cbs.S3RawSnapshotWriter", FakeS3Writer)
    monkeypatch.setattr("fip.commands.cbs.LocalManifestWriter", FakeLocalManifestWriter)
    monkeypatch.setattr("fip.commands.cbs.S3ManifestWriter", FakeS3ManifestWriter)
    monkeypatch.setattr("fip.commands.cbs.SourceRunLandingWriter", FakeSourceRunLandingWriter)

    result = runner.invoke(
        cli.app,
        [
            "archive-cbs-raw",
            "--table-id",
            "83625NED",
            "--run-id",
            "debug-raw",
            "--target",
            target,
            "--output-dir",
            ".raw-smoke",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 raw records\n"
    assert calls["table_id"] == "83625NED"
    assert calls["run_id"] == "debug-raw"
    assert calls["writer"] == expected_writer
    rows = cast(list[RawRecord], calls["rows"])
    assert len(rows) == 1
    assert rows[0].entity_name == "83625NED.MeasureCodes"
    if target == "local":
        assert calls["base_dir"] == ".raw-smoke"
        assert calls["manifest_base_dir"] == Path(".raw-smoke")
    assert calls["manifest_writer"] == expected_writer
    assert calls["manifest_table_id"] == "83625NED"
    assert calls["source_run_writer"] == "called"
    assert len(calls["source_run_manifests"]) == 1
    manifest = calls["manifest"]
    assert manifest.source_name == "cbs_statline"
    assert manifest.source_family == "cbs"
    assert manifest.run_id == "debug-raw"
    assert manifest.source_version == "83625NED"
    assert manifest.row_count == 1
    assert manifest.status == "success"
    assert manifest.error_message is None
    assert manifest.source_url == "https://opendata.cbs.nl/ODataApi/OData/83625NED"
    assert manifest.license == "cbs_open_data"
    assert manifest.attribution == "CBS StatLine"
    if target == "local":
        assert (
            Path(manifest.raw_uri) == Path(".raw-smoke") / "raw" / "cbs" / "83625NED" / "debug-raw"
        )
    else:
        assert manifest.raw_uri.endswith("/raw/cbs/83625NED/debug-raw/")


@pytest.mark.parametrize(
    ("target", "expected_writer"),
    [
        ("local", "local"),
        ("s3", "s3"),
    ],
)
def test_archive_bag_gpkg_command_wires_source_and_writes_jsonl(
    monkeypatch, target: str, expected_writer: str
) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        def __init__(
            self,
            run_id: str,
            source_ref: str,
            layer: str = "verblijfsobject",
            max_features: int | None = None,
        ) -> None:
            calls["run_id"] = run_id
            calls["source_ref"] = source_ref
            calls["layer"] = layer
            calls["max_features"] = max_features

        def iter_records(self):
            yield RawRecord(
                source_name="bag_gpkg",
                entity_name="bag_gpkg.verblijfsobject",
                natural_key="0003010000126809",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-gpkg",
                payload={
                    "feature_id": 1,
                    "identificatie": "0003010000126809",
                    "geometry": {"type": "Point", "coordinates": [12345, 456789]},
                },
                schema_version="v1",
                http_status=200,
            )

    class FakeLocalWriter:
        def __init__(self, base_dir: str) -> None:
            calls["writer"] = "local"
            calls["base_dir"] = base_dir

        def open_for_record(self, record: RawRecord):
            calls["record"] = record

            class FakeHandle:
                def write(self, value: str) -> None:
                    rows = cast(list[str], calls.setdefault("rows", []))
                    if value != "\n":
                        rows.append(value)

                def close(self) -> None:
                    return None

            return FakeHandle()

    class FakeS3Writer:
        def __init__(self) -> None:
            calls["writer"] = "s3"

        def open_for_record(self, record: RawRecord):
            calls["record"] = record

            class FakeHandle:
                def write(self, value: str) -> None:
                    rows = cast(list[str], calls.setdefault("rows", []))
                    if value != "\n":
                        rows.append(value)

                def close(self) -> None:
                    return None

            return FakeHandle()

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

    monkeypatch.setattr("fip.commands.pdok_bag.PDOKBAGGeoPackageSource", FakeSource)
    monkeypatch.setattr("fip.commands.pdok_bag.RawSnapshotWriter", FakeLocalWriter)
    monkeypatch.setattr("fip.commands.pdok_bag.S3RawSnapshotWriter", FakeS3Writer)
    monkeypatch.setattr("fip.commands.pdok_bag.LocalManifestWriter", FakeLocalManifestWriter)
    monkeypatch.setattr("fip.commands.pdok_bag.S3ManifestWriter", FakeS3ManifestWriter)
    monkeypatch.setattr("fip.commands.pdok_bag.SourceRunLandingWriter", FakeSourceRunLandingWriter)

    result = runner.invoke(
        cli.app,
        [
            "archive-bag-gpkg",
            "--run-id",
            "debug-gpkg",
            "--source-ref",
            "data/pdok-bag/bag-light.gpkg",
            "--layer",
            "verblijfsobject",
            "--target",
            target,
            "--limit",
            "1",
            "--output-dir",
            ".raw-smoke",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 raw records\n"
    assert calls["run_id"] == "debug-gpkg"
    assert calls["source_ref"] == "data/pdok-bag/bag-light.gpkg"
    assert calls["layer"] == "verblijfsobject"
    assert calls["max_features"] == 1
    assert calls["writer"] == expected_writer
    rows = cast(list[str], calls["rows"])
    assert len(rows) == 1
    assert '"entity_name": "bag_gpkg.verblijfsobject"' in rows[0]
    assert '"source_name": "bag_gpkg"' in rows[0]
    if target == "local":
        assert calls["base_dir"] == ".raw-smoke"
        assert calls["manifest_base_dir"] == Path(".raw-smoke")
    assert calls["manifest_writer"] == expected_writer
    assert calls["manifest_table_id"] is None
    assert calls["source_run_writer"] == "called"
    assert len(calls["source_run_manifests"]) == 1
    manifest = calls["manifest"]
    assert manifest.source_name == "bag_gpkg"
    assert manifest.source_family == "pdok_bag"
    assert manifest.run_id == "debug-gpkg"
    assert manifest.source_version == "verblijfsobject"
    assert manifest.row_count == 1
    assert manifest.status == "success"
    assert manifest.error_message is None
    assert manifest.source_url == "data/pdok-bag/bag-light.gpkg"
    assert manifest.license == "pdok_open_data"
    assert manifest.attribution == "PDOK / Kadaster BAG"
    if target == "local":
        assert Path(manifest.raw_uri) == Path(".raw-smoke") / "raw" / "bag_gpkg" / "debug-gpkg"
    else:
        assert manifest.raw_uri.endswith("/raw/bag_gpkg/debug-gpkg/")


@pytest.mark.parametrize(
    ("target", "expected_writer"),
    [
        ("local", "local"),
        ("s3", "s3"),
    ],
)
def test_archive_onderwijsinspectie_raw_command_writes_records(
    monkeypatch, target: str, expected_writer: str
) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        name = "onderwijsinspectie"
        schema_version = "v1"

        def __init__(self, source_ref: str, run_id: str) -> None:
            calls["source_ref"] = source_ref
            calls["run_id"] = run_id

        def iter_records(self):
            yield RawRecord(
                source_name="onderwijsinspectie",
                entity_name="inspection.schools",
                natural_key="school-001",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="inspectie-load",
                payload={"school_id": "school-001", "school_name": "De School"},
                schema_version="v1",
                http_status=200,
            )

    class FakeLocalWriter:
        def __init__(self, base_dir: str) -> None:
            calls["writer"] = "local"
            calls["base_dir"] = base_dir

        def open_for_record(self, record: RawRecord):
            calls["record"] = record

            class FakeHandle:
                def write(self, value: str) -> None:
                    rows = cast(list[str], calls.setdefault("rows", []))
                    if value != "\n":
                        rows.append(value)

                def close(self) -> None:
                    return None

            return FakeHandle()

    class FakeS3Writer:
        def __init__(self) -> None:
            calls["writer"] = "s3"

        def open_for_record(self, record: RawRecord):
            calls["record"] = record

            class FakeHandle:
                def write(self, value: str) -> None:
                    rows = cast(list[str], calls.setdefault("rows", []))
                    if value != "\n":
                        rows.append(value)

                def close(self) -> None:
                    return None

            return FakeHandle()

    class FakeLocalManifestWriter:
        def __init__(self, base_dir: str) -> None:
            calls["manifest_writer"] = "local"
            calls["manifest_base_dir"] = base_dir

        def write(self, manifest):
            calls["manifest"] = manifest
            return None

    class FakeS3ManifestWriter:
        def __init__(self) -> None:
            calls["manifest_writer"] = "s3"

        def write(self, manifest):
            calls["manifest"] = manifest
            return None

    class FakeSourceRunLandingWriter:
        def write(self, manifests) -> int:
            rows = list(manifests)
            calls["source_run_writer"] = "called"
            calls["source_run_manifests"] = rows
            return len(rows)

    monkeypatch.setattr("fip.commands.onderwijsinspectie.OnderwijsInspectieSource", FakeSource)
    monkeypatch.setattr("fip.commands.onderwijsinspectie.RawSnapshotWriter", FakeLocalWriter)
    monkeypatch.setattr("fip.commands.onderwijsinspectie.S3RawSnapshotWriter", FakeS3Writer)
    monkeypatch.setattr(
        "fip.commands.onderwijsinspectie.LocalManifestWriter",
        FakeLocalManifestWriter,
    )
    monkeypatch.setattr(
        "fip.commands.onderwijsinspectie.S3ManifestWriter",
        FakeS3ManifestWriter,
    )
    monkeypatch.setattr(
        "fip.commands.onderwijsinspectie.SourceRunLandingWriter",
        FakeSourceRunLandingWriter,
    )

    result = runner.invoke(
        cli.app,
        [
            "archive-onderwijsinspectie-raw",
            "--source-ref",
            "data/onderwijsinspectie/sample.csv",
            "--run-id",
            "inspectie-load",
            "--target",
            target,
            "--output-dir",
            ".raw-smoke",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 raw records\n"
    assert calls["source_ref"] == "data/onderwijsinspectie/sample.csv"
    assert calls["run_id"] == "inspectie-load"
    assert calls["writer"] == expected_writer
    rows = cast(list[str], calls["rows"])
    assert len(rows) == 1
    assert '"entity_name": "inspection.schools"' in rows[0]
    assert '"source_name": "onderwijsinspectie"' in rows[0]
    if target == "local":
        assert calls["base_dir"] == ".raw-smoke"
        assert calls["manifest_base_dir"] == Path(".raw-smoke")
    assert calls["manifest_writer"] == expected_writer
    assert calls["source_run_writer"] == "called"
    assert len(calls["source_run_manifests"]) == 1
    manifest = calls["manifest"]
    assert manifest.source_name == "onderwijsinspectie"
    assert manifest.source_family == "inspection"
    assert manifest.run_id == "inspectie-load"
    assert manifest.source_version == "v1"
    assert manifest.row_count == 1
    assert manifest.status == "success"
    assert manifest.error_message is None
    assert manifest.source_url == "data/onderwijsinspectie/sample.csv"
    assert manifest.license == "unknown"
    assert manifest.attribution == "Inspectie van het Onderwijs"
    if target == "local":
        assert (
            Path(manifest.raw_uri) == Path(".raw-smoke") / "raw" / "inspection" / "inspectie-load"
        )
    else:
        assert manifest.raw_uri.endswith("/raw/inspection/inspectie-load/")


@pytest.mark.parametrize(
    ("command", "expected_entity", "expected_table_name", "expected_natural_key"),
    [
        (
            "build-gold-measure-codes",
            "MeasureCodes",
            "cbs_measure_codes",
            "M001534",
        ),
        (
            "build-gold-period-codes",
            "PeriodenCodes",
            "cbs_period_codes",
            "1995JJ00",
        ),
        (
            "build-gold-region-codes",
            "RegioSCodes",
            "cbs_region_codes",
            "NL01",
        ),
        (
            "build-gold-eigendom-codes",
            "EigendomCodes",
            "cbs_eigendom_codes",
            "A047047",
        ),
    ],
)
def test_build_gold_reference_commands_filter_records_and_write(
    monkeypatch,
    command: str,
    expected_entity: str,
    expected_table_name: str,
    expected_natural_key: str,
) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        def __init__(self, base_dir=None) -> None:
            calls["base_dir"] = base_dir

        def iter_cbs_entity_records(self, table_id: str, run_id: str, entity: str):
            calls["table_id"] = table_id
            calls["run_id"] = run_id
            calls["entity"] = entity
            yield RawRecord(
                source_name="cbs_statline",
                entity_name=f"83625NED.{expected_entity}",
                natural_key=expected_natural_key,
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"Identifier": expected_natural_key, "Title": "Matched"},
                schema_version="v1",
            )

    class FakeWriter:
        def __init__(self, table_name: str, entity: str) -> None:
            calls["table_name"] = table_name
            calls["entity"] = entity
            self.rows: list[RawRecord] = []

        def write(self, rows):
            self.rows = list(rows)
            calls["rows"] = self.rows
            return len(self.rows)

    monkeypatch.setattr("fip.commands._helpers.RawSnapshotReader", FakeSource)
    monkeypatch.setattr("fip.commands._helpers.S3RawSnapshotReader", FakeSource)
    monkeypatch.setattr("fip.commands._helpers.CBSReferenceCodeWriter", FakeWriter)

    result = runner.invoke(
        cli.app,
        [
            command,
            "--table-id",
            "83625NED",
            "--run-id",
            "debug-raw",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == f"Wrote 1 {expected_entity} rows into {expected_table_name}\n"
    assert calls["table_id"] == "83625NED"
    assert calls["run_id"] == "debug-raw"
    assert calls["table_name"] == expected_table_name
    assert calls["entity"] == expected_entity
    rows = cast(list[RawRecord], calls["rows"])
    assert len(rows) == 1
    assert rows[0].entity_name == f"83625NED.{expected_entity}"
    assert rows[0].natural_key == expected_natural_key


def test_inspect_bronze_command_prints_row_count_and_rows(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeConnection:
        def close(self) -> None:
            calls["closed"] = True

    def fake_connect_duckdb() -> FakeConnection:
        calls["connected"] = True
        return FakeConnection()

    def fake_load_extensions(con: object) -> None:
        calls["extensions_loaded"] = con

    def fake_attach_iceberg_catalog(con: object) -> None:
        calls["catalog_attached"] = con

    def fake_count_rows(con: object, table_name: str, namespace: str | None) -> int:
        calls["count_table_name"] = table_name
        calls["count_namespace"] = namespace
        return 23095

    def fake_sample_rows(
        con: object,
        table_name: str,
        namespace: str | None,
        limit: int,
    ) -> list[tuple[str, str]]:
        calls["sample_table_name"] = table_name
        calls["sample_namespace"] = namespace
        calls["sample_limit"] = limit
        return [("row-1", "value-1"), ("row-2", "value-2")]

    monkeypatch.setattr("fip.commands.inspection.connect_duckdb", fake_connect_duckdb)
    monkeypatch.setattr("fip.commands.inspection.load_extensions", fake_load_extensions)
    monkeypatch.setattr(
        "fip.commands.inspection.attach_iceberg_catalog",
        fake_attach_iceberg_catalog,
    )
    monkeypatch.setattr("fip.commands.inspection.count_rows", fake_count_rows)
    monkeypatch.setattr("fip.commands.inspection.sample_rows", fake_sample_rows)

    result = runner.invoke(
        cli.app,
        [
            "inspect-bronze",
            "--table",
            "cbs_observations_83625ned",
            "--limit",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == (
        "Row count: 23095\nSample rows (2):\n('row-1', 'value-1')\n('row-2', 'value-2')\n"
    )
    assert calls["connected"] is True
    assert calls["count_table_name"] == "cbs_observations_83625ned"
    assert calls["count_namespace"] is None
    assert calls["sample_table_name"] == "cbs_observations_83625ned"
    assert calls["sample_namespace"] is None
    assert calls["sample_limit"] == 2
    assert calls["closed"] is True


def test_build_silver_observations_command_reads_bronze_and_writes_silver(
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeSilverSink:
        def __init__(self, table_ident: str) -> None:
            self.table_ident = table_ident
            calls["table_ident"] = table_ident

    def fake_read_bronze_rows(
        table_name: str,
        namespace: str | None,
        run_id: str | None,
    ) -> list[dict[str, object]]:
        calls["read_table_name"] = table_name
        calls["read_namespace"] = namespace
        calls["read_run_id"] = run_id
        return [
            {
                "source_name": "cbs_statline",
                "natural_key": "1",
                "retrieved_at": "2026-04-18T09:00:00Z",
                "run_id": "run-001",
                "schema_version": "v1",
                "http_status": 200,
                "payload": (
                    '{"Id": 1, "Measure": "M001534", "Perioden": '
                    '"1995JJ00", "RegioS": "NL01", "StringValue": null, '
                    '"Value": 93750.0, "ValueAttribute": "None"}'
                ),
            }
        ]

    def fake_write_bronze_rows_to_cbs_observation_sink(
        bronze_rows: list[dict[str, object]],
        sink: object,
    ) -> int:
        calls["bronze_rows"] = bronze_rows
        calls["sink"] = sink
        return 1

    monkeypatch.setattr("fip.commands.cbs.read_bronze_rows", fake_read_bronze_rows)
    monkeypatch.setattr("fip.commands.cbs.CBSObservationSink", FakeSilverSink)
    monkeypatch.setattr(
        "fip.commands.cbs.write_bronze_rows_to_cbs_observation_sink",
        fake_write_bronze_rows_to_cbs_observation_sink,
    )

    result = runner.invoke(
        cli.app,
        [
            "build-cbs-silver-observations",
            "--table",
            "cbs_observations_83625ned",
            "--silver-table",
            "cbs_observations_flat_83625ned",
            "--run-id",
            "smoke-load",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 Silver rows\n"
    assert calls["read_table_name"] == "cbs_observations_83625ned"
    assert calls["read_namespace"] is None
    assert calls["read_run_id"] == "smoke-load"
    assert calls["table_ident"] == "silver.cbs_observations_flat_83625ned"
    bronze_rows = cast(list[dict[str, object]], calls["bronze_rows"])
    assert len(bronze_rows) == 1
    sink = calls["sink"]
    assert isinstance(sink, FakeSilverSink)
    assert sink.table_ident == "silver.cbs_observations_flat_83625ned"


def test_build_cbs_silver_observations_command_supports_custom_silver_table(
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeSilverSink:
        def __init__(self, table_ident: str) -> None:
            self.table_ident = table_ident
            calls["table_ident"] = table_ident

    def fake_read_bronze_rows(
        table_name: str,
        namespace: str | None,
        run_id: str | None,
    ) -> list[dict[str, object]]:
        calls["read_table_name"] = table_name
        calls["read_namespace"] = namespace
        calls["read_run_id"] = run_id
        return [
            {
                "source_name": "cbs_statline",
                "natural_key": "1",
                "retrieved_at": "2026-04-18T09:00:00Z",
                "run_id": "run-001",
                "schema_version": "v1",
                "http_status": 200,
                "payload": (
                    '{"Id": 1, "Measure": "M001534", "Perioden": '
                    '"1995JJ00", "RegioS": "NL01", "StringValue": null, '
                    '"Value": 93750.0, "ValueAttribute": "None"}'
                ),
            }
        ]

    def fake_write_bronze_rows_to_cbs_observation_sink(
        bronze_rows: list[dict[str, object]],
        sink: object,
    ) -> int:
        calls["bronze_rows"] = bronze_rows
        calls["sink"] = sink
        return 1

    monkeypatch.setattr("fip.commands.cbs.read_bronze_rows", fake_read_bronze_rows)
    monkeypatch.setattr("fip.commands.cbs.CBSObservationSink", FakeSilverSink)
    monkeypatch.setattr(
        "fip.commands.cbs.write_bronze_rows_to_cbs_observation_sink",
        fake_write_bronze_rows_to_cbs_observation_sink,
    )

    result = runner.invoke(
        cli.app,
        [
            "build-cbs-silver-observations",
            "--table",
            "cbs_observations_85036ned",
            "--silver-table",
            "cbs_observations_flat_85036ned",
            "--run-id",
            "woz-load",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 Silver rows\n"
    assert calls["read_table_name"] == "cbs_observations_85036ned"
    assert calls["read_namespace"] is None
    assert calls["read_run_id"] == "woz-load"
    assert calls["table_ident"] == "silver.cbs_observations_flat_85036ned"
    bronze_rows = cast(list[dict[str, object]], calls["bronze_rows"])
    assert len(bronze_rows) == 1
    sink = calls["sink"]
    assert isinstance(sink, FakeSilverSink)
    assert sink.table_ident == "silver.cbs_observations_flat_85036ned"


def test_build_bag_gpkg_silver_verblijfsobject_command_reads_bronze_and_writes_silver(
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeBagGpkgSilverSink:
        def __init__(self, table_ident: str) -> None:
            self.table_ident = table_ident
            calls["table_ident"] = table_ident

    def fake_read_bronze_rows(
        table_name: str,
        namespace: str | None,
        run_id: str | None,
    ) -> list[dict[str, object]]:
        calls["read_table_name"] = table_name
        calls["read_namespace"] = namespace
        calls["read_run_id"] = run_id
        return [
            {
                "source_name": "bag_gpkg",
                "natural_key": "1",
                "retrieved_at": "2026-04-19T17:43:42.077000Z",
                "run_id": "run-001",
                "schema_version": "v1",
                "http_status": 200,
                "payload": "{}",
            }
        ]

    def fake_write_bronze_rows_to_bag_gpkg_verblijfsobject_sink(
        bronze_rows: list[dict[str, object]],
        sink: object,
    ) -> int:
        calls["bronze_rows"] = bronze_rows
        calls["sink"] = sink
        return 1

    monkeypatch.setattr("fip.commands.pdok_bag.read_bronze_rows", fake_read_bronze_rows)
    monkeypatch.setattr("fip.commands.pdok_bag.BAGGpkgVerblijfsobjectSink", FakeBagGpkgSilverSink)
    monkeypatch.setattr(
        "fip.commands.pdok_bag.write_bronze_rows_to_bag_gpkg_verblijfsobject_sink",
        fake_write_bronze_rows_to_bag_gpkg_verblijfsobject_sink,
    )

    result = runner.invoke(
        cli.app,
        [
            "build-bag-gpkg-silver-verblijfsobject",
            "--table",
            "bag_gpkg_verblijfsobject",
            "--run-id",
            "smoke-load",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 BAG GPKG Silver rows\n"
    assert calls["read_table_name"] == "bag_gpkg_verblijfsobject"
    assert calls["read_namespace"] is None
    assert calls["read_run_id"] == "smoke-load"
    assert calls["table_ident"] == "silver.bag_gpkg_verblijfsobject_flat"
    bronze_rows = cast(list[dict[str, object]], calls["bronze_rows"])
    assert len(bronze_rows) == 1
    sink = calls["sink"]
    assert isinstance(sink, FakeBagGpkgSilverSink)
    assert sink.table_ident == "silver.bag_gpkg_verblijfsobject_flat"


def test_build_bag_gpkg_landing_verblijfsobject_command_reads_silver_and_writes_landing(
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeBAGGpkgLandingWriter:
        def __init__(self, table_name: str) -> None:
            self.table_name = table_name
            calls["table_name"] = table_name

    def fake_read_silver_rows(
        table_name: str,
        namespace: str | None,
        run_id: str | None,
    ) -> list[dict[str, object]]:
        calls["read_table_name"] = table_name
        calls["read_namespace"] = namespace
        calls["read_run_id"] = run_id
        return [
            {
                "source_name": "bag_gpkg",
                "natural_key": "1",
                "retrieved_at": "2026-04-19T17:43:42.077000Z",
                "run_id": "run-001",
                "schema_version": "v1",
                "http_status": 200,
                "bag_id": "bag-1",
                "verblijfsobject_identificatie": "0000010000057469",
                "hoofdadres_identificatie": "0000200000057534",
                "postcode": "6131BE",
                "huisnummer": 32,
                "huisletter": "A",
                "toevoeging": None,
                "woonplaats_naam": "Sittard",
                "openbare_ruimte_naam": "Steenweg",
                "gebruiksdoel": "woonfunctie",
                "oppervlakte": 72,
                "geometry": '{"type": "Point"}',
            }
        ]

    def fake_write_rows_to_sink(
        silver_rows: list[dict[str, object]],
        sink: object,
    ) -> int:
        calls["silver_rows"] = silver_rows
        calls["sink"] = sink
        return len(silver_rows)

    monkeypatch.setattr("fip.commands.pdok_bag.read_silver_rows", fake_read_silver_rows)
    monkeypatch.setattr(
        "fip.commands.pdok_bag.BAGGpkgVerblijfsobjectLandingWriter",
        FakeBAGGpkgLandingWriter,
    )
    monkeypatch.setattr("fip.commands.pdok_bag.write_rows_to_sink", fake_write_rows_to_sink)

    result = runner.invoke(
        cli.app,
        [
            "build-bag-gpkg-landing-verblijfsobject",
            "--table",
            "bag_gpkg_verblijfsobject_flat",
            "--run-id",
            "smoke-load",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 BAG GPKG landing rows\n"
    assert calls["read_table_name"] == "bag_gpkg_verblijfsobject_flat"
    assert calls["read_namespace"] is None
    assert calls["read_run_id"] == "smoke-load"
    assert calls["table_name"] == "bag_gpkg_verblijfsobject"
    silver_rows = cast(list[dict[str, object]], calls["silver_rows"])
    assert len(silver_rows) == 1
    sink = calls["sink"]
    assert isinstance(sink, FakeBAGGpkgLandingWriter)
    assert sink.table_name == "bag_gpkg_verblijfsobject"


def test_inspect_silver_command_prints_row_count_and_rows(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeConnection:
        def close(self) -> None:
            calls["closed"] = True

    def fake_connect_duckdb() -> FakeConnection:
        calls["connected"] = True
        return FakeConnection()

    def fake_load_extensions(con: object) -> None:
        calls["extensions_loaded"] = con

    def fake_attach_iceberg_catalog(con: object) -> None:
        calls["catalog_attached"] = con

    def fake_count_rows(con: object, table_name: str, namespace: str | None) -> int:
        calls["count_table_name"] = table_name
        calls["count_namespace"] = namespace
        return 42

    def fake_sample_rows(
        con: object,
        table_name: str,
        namespace: str | None,
        limit: int,
    ) -> list[tuple[str, str]]:
        calls["sample_table_name"] = table_name
        calls["sample_namespace"] = namespace
        calls["sample_limit"] = limit
        return [("silver-row-1", "value-1")]

    monkeypatch.setattr("fip.commands.inspection.connect_duckdb", fake_connect_duckdb)
    monkeypatch.setattr("fip.commands.inspection.load_extensions", fake_load_extensions)
    monkeypatch.setattr(
        "fip.commands.inspection.attach_iceberg_catalog",
        fake_attach_iceberg_catalog,
    )
    monkeypatch.setattr("fip.commands.inspection.count_rows", fake_count_rows)
    monkeypatch.setattr("fip.commands.inspection.sample_rows", fake_sample_rows)

    result = runner.invoke(
        cli.app,
        [
            "inspect-cbs-silver",
            "--table",
            "cbs_observations_flat_83625ned",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Row count: 42\nSample rows (1):\n('silver-row-1', 'value-1')\n"
    assert calls["connected"] is True
    assert calls["count_table_name"] == "cbs_observations_flat_83625ned"
    assert calls["count_namespace"] == "silver"
    assert calls["sample_table_name"] == "cbs_observations_flat_83625ned"
    assert calls["sample_namespace"] == "silver"
    assert calls["sample_limit"] == 1
    assert calls["closed"] is True


def test_build_gold_observations_command_reads_silver_and_writes_gold(
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeGoldSink:
        def __init__(self, table_name: str) -> None:
            self.table_name = table_name
            calls["table_name"] = table_name

    def fake_read_silver_rows(
        table_name: str,
        namespace: str | None,
        run_id: str | None,
    ) -> list[dict[str, object]]:
        calls["read_table_name"] = table_name
        calls["read_namespace"] = namespace
        calls["read_run_id"] = run_id
        return [
            {
                "source_name": "cbs_statline",
                "natural_key": "1",
                "retrieved_at": "2026-04-18T09:00:00Z",
                "run_id": "run-001",
                "schema_version": "v1",
                "http_status": 200,
                "observation_id": 1,
                "measure_code": "M001534",
                "eigendom_code": None,
                "period_code": "1995JJ00",
                "region_code": "NL01",
                "numeric_value": 93750.0,
                "value_attribute": "None",
                "string_value": None,
                "woningtype_code": None,
                "woningkenmerk_code": None,
            }
        ]

    def fake_write_rows_to_sink(
        silver_rows: list[dict[str, object]],
        sink: object,
    ) -> int:
        calls["silver_rows"] = silver_rows
        calls["sink"] = sink
        return 1

    monkeypatch.setattr("fip.commands.cbs.read_silver_rows", fake_read_silver_rows)
    monkeypatch.setattr("fip.commands.cbs.CBSObservationLandingWriter", FakeGoldSink)
    monkeypatch.setattr("fip.commands.cbs.write_rows_to_sink", fake_write_rows_to_sink)

    result = runner.invoke(
        cli.app,
        [
            "build-landing-observations",
            "--table",
            "cbs_observations_flat_83625ned",
            "--run-id",
            "smoke-load",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 landing rows\n"
    assert calls["read_table_name"] == "cbs_observations_flat_83625ned"
    assert calls["read_namespace"] is None
    assert calls["read_run_id"] == "smoke-load"
    assert calls["table_name"] == "cbs_observations_83625ned"
    silver_rows = cast(list[dict[str, object]], calls["silver_rows"])
    assert len(silver_rows) == 1
    sink = calls["sink"]
    assert isinstance(sink, FakeGoldSink)
    assert sink.table_name == "cbs_observations_83625ned"


def test_build_landing_observations_command_supports_custom_landing_table(
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeGoldSink:
        def __init__(self, table_name: str) -> None:
            self.table_name = table_name
            calls["table_name"] = table_name

    def fake_read_silver_rows(
        table_name: str,
        namespace: str | None,
        run_id: str | None,
    ) -> list[dict[str, object]]:
        calls["read_table_name"] = table_name
        calls["read_namespace"] = namespace
        calls["read_run_id"] = run_id
        return [
            {
                "source_name": "cbs_statline",
                "natural_key": "1",
                "retrieved_at": "2026-04-18T09:00:00Z",
                "run_id": "run-001",
                "schema_version": "v1",
                "http_status": 200,
                "observation_id": 1,
                "measure_code": "M001534",
                "eigendom_code": None,
                "period_code": "1995JJ00",
                "region_code": "NL01",
                "numeric_value": 93750.0,
                "value_attribute": "None",
                "string_value": None,
                "woningtype_code": None,
                "woningkenmerk_code": None,
            }
        ]

    def fake_write_rows_to_sink(
        silver_rows: list[dict[str, object]],
        sink: object,
    ) -> int:
        calls["silver_rows"] = silver_rows
        calls["sink"] = sink
        return 1

    monkeypatch.setattr("fip.commands.cbs.read_silver_rows", fake_read_silver_rows)
    monkeypatch.setattr("fip.commands.cbs.CBSObservationLandingWriter", FakeGoldSink)
    monkeypatch.setattr("fip.commands.cbs.write_rows_to_sink", fake_write_rows_to_sink)

    result = runner.invoke(
        cli.app,
        [
            "build-landing-observations",
            "--table",
            "cbs_observations_flat_85036ned",
            "--landing-table",
            "cbs_observations_85036ned",
            "--run-id",
            "woz-load",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 landing rows\n"
    assert calls["read_table_name"] == "cbs_observations_flat_85036ned"
    assert calls["read_namespace"] is None
    assert calls["read_run_id"] == "woz-load"
    assert calls["table_name"] == "cbs_observations_85036ned"
    silver_rows = cast(list[dict[str, object]], calls["silver_rows"])
    assert len(silver_rows) == 1
    sink = calls["sink"]
    assert isinstance(sink, FakeGoldSink)
    assert sink.table_name == "cbs_observations_85036ned"


def test_inspect_gold_command_prints_row_count_and_rows(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeConnection:
        def close(self) -> None:
            calls["closed"] = True

    def fake_connect_postgres() -> FakeConnection:
        calls["connected"] = True
        return FakeConnection()

    def fake_count_gold_rows(conn: object, table_name: str, schema: str | None) -> int:
        calls["count_table_name"] = table_name
        calls["count_schema"] = schema
        return 3

    def fake_sample_gold_rows(
        conn: object,
        table_name: str,
        schema: str | None,
        limit: int,
    ) -> list[tuple[str, str]]:
        calls["sample_table_name"] = table_name
        calls["sample_schema"] = schema
        calls["sample_limit"] = limit
        return [("gold-row-1", "value-1")]

    monkeypatch.setattr("fip.commands.inspection.connect_postgres", fake_connect_postgres)
    monkeypatch.setattr("fip.commands.inspection.count_gold_rows", fake_count_gold_rows)
    monkeypatch.setattr("fip.commands.inspection.sample_gold_rows", fake_sample_gold_rows)

    result = runner.invoke(
        cli.app,
        [
            "inspect-landing",
            "--table",
            "cbs_observations_83625ned",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Row count: 3\nSample rows (1):\n('gold-row-1', 'value-1')\n"
    assert calls["connected"] is True
    assert calls["count_table_name"] == "cbs_observations_83625ned"
    assert calls["count_schema"] is None
    assert calls["sample_table_name"] == "cbs_observations_83625ned"
    assert calls["sample_schema"] is None
    assert calls["sample_limit"] == 1
    assert calls["closed"] is True
