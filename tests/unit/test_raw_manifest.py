from datetime import datetime, timezone
from pathlib import Path

from fip.raw.manifest import (
    LocalManifestWriter,
    SourceRunManifest,
    local_manifest_path,
    s3_manifest_uri,
)


def make_manifest() -> SourceRunManifest:
    return SourceRunManifest(
        source_name="cbs_statline",
        source_family="cbs",
        run_id="run-001",
        started_at=datetime(2026, 4, 29, 10, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 4, 29, 10, 1, tzinfo=timezone.utc),
        source_url="https://opendata.cbs.nl/ODataApi/OData/83625NED",
        source_version="83625NED",
        license="cbs_open_data",
        attribution="CBS StatLine",
        raw_uri="s3://fip-lakehouse/raw/cbs/83625NED/run-001/Observations.jsonl",
        row_count=123,
        status="success",
    )


def test_manifest_roundtrip_json() -> None:
    manifest = make_manifest()

    loaded = SourceRunManifest.from_json(manifest.to_json())

    assert loaded == manifest


def test_local_manifest_path_for_cbs(tmp_path: Path) -> None:
    path = local_manifest_path(
        base_dir=tmp_path,
        source_name="cbs_statline",
        run_id="run-001",
        table_id="83625NED",
    )

    assert path == tmp_path / "raw" / "cbs" / "83625NED" / "run-001" / "manifest.json"


def test_local_manifest_path_for_cbs_crime(tmp_path: Path) -> None:
    path = local_manifest_path(
        base_dir=tmp_path,
        source_name="cbs_crime",
        run_id="run-001",
        table_id="83648NED",
    )

    assert path == tmp_path / "raw" / "cbs" / "83648NED" / "run-001" / "manifest.json"


def test_s3_manifest_uri_for_bag_gpkg() -> None:
    uri = s3_manifest_uri(
        bucket="fip-lakehouse",
        source_name="bag_gpkg",
        run_id="run-001",
    )

    assert uri == "s3://fip-lakehouse/raw/bag_gpkg/run-001/manifest.json"


def test_local_manifest_writer_writes_manifest(tmp_path: Path) -> None:
    writer = LocalManifestWriter(base_dir=tmp_path)
    manifest = make_manifest()

    path = writer.write(manifest, table_id="83625NED")

    assert path.exists()
    assert SourceRunManifest.from_json(path.read_text(encoding="utf-8")) == manifest
