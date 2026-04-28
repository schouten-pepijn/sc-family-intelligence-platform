from __future__ import annotations

import json
from datetime import date, datetime
from typing import cast

from typer.testing import CliRunner

from fip import cli

runner = CliRunner()


def _make_pdok_payload() -> dict[str, object]:
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "gemeentecode": "GM0363",
                    "gemeentenaam": "Amsterdam",
                    "jaar": 2025,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [4.8, 52.3],
                            [4.9, 52.3],
                            [4.9, 52.4],
                            [4.8, 52.4],
                            [4.8, 52.3],
                        ]
                    ],
                },
            }
        ],
    }


def _make_pdok_payload_with_buitenland() -> dict[str, object]:
    payload = _make_pdok_payload()
    features = cast(list[dict[str, object]], payload["features"])
    payload["features"] = features + [
        {
            "type": "Feature",
            "properties": {
                "gemeentecode": "GM0998",
                "gemeentenaam": "Buitenland",
                "jaar": 2025,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0.0, 0.0], [0.1, 0.0], [0.1, 0.1], [0.0, 0.1], [0.0, 0.0]]],
            },
        }
    ]
    return payload


def _make_archived_payload() -> dict[str, object]:
    return {
        "type": "FeatureCollection",
        "metadata": {
            "source_name": "pdok_cbs_wijken_en_buurten",
            "source_year": 2025,
            "collection": "gemeenten",
            "run_id": "region-geom-2025",
        },
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "source_name": "pdok_cbs_wijken_en_buurten",
                    "natural_key": "GM0363|2025",
                    "retrieved_at": "2025-04-26T12:00:00+00:00",
                    "run_id": "region-geom-2025",
                    "schema_version": "v1",
                    "http_status": 200,
                    "region_id": "GM0363",
                    "region_level": "municipality",
                    "region_name": "Amsterdam",
                    "source_version": "2025",
                    "valid_from": "2025-01-01",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [4.8, 52.3],
                            [4.9, 52.3],
                            [4.9, 52.4],
                            [4.8, 52.4],
                            [4.8, 52.3],
                        ]
                    ],
                },
            }
        ],
    }


def test_archive_region_geom_command_writes_normalized_geojson_to_local_target(
    tmp_path,
    monkeypatch,
) -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object], url: str) -> None:
            self._payload = payload
            self.url = url
            self.links: dict[str, dict[str, str]] = {}

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeClient:
        def __init__(self, timeout: float) -> None:
            self.timeout = timeout
            self.calls: list[str] = []

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, url: str) -> FakeResponse:
            self.calls.append(url)
            return FakeResponse(_make_pdok_payload(), url)

    fake_client = FakeClient(timeout=60.0)
    monkeypatch.setattr("fip.commands.gis.httpx.Client", lambda timeout: fake_client)

    result = runner.invoke(
        cli.app,
        [
            "archive-region-geom",
            "--year",
            "2025",
            "--run-id",
            "region-geom-2025",
            "--target",
            "local",
            "--output-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    assert result.stdout.startswith("Archived region geometry to ")
    assert result.stdout.endswith("gemeenten.geojson\n")
    assert fake_client.calls == [
        "https://api.pdok.nl/cbs/wijken-en-buurten-2025/ogc/v1/collections/gemeenten/items?f=json&limit=1000"
    ]

    output_file = (
        tmp_path
        / "raw"
        / "region_geom"
        / "cbs-wijken-en-buurten-2025"
        / "region-geom-2025"
        / "gemeenten.geojson"
    )
    payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert payload["type"] == "FeatureCollection"
    assert payload["metadata"]["source_name"] == "pdok_cbs_wijken_en_buurten"
    feature = payload["features"][0]
    assert feature["properties"]["source_name"] == "pdok_cbs_wijken_en_buurten"
    assert feature["properties"]["natural_key"] == "GM0363|2025"
    assert feature["properties"]["region_id"] == "GM0363"
    assert feature["properties"]["region_level"] == "municipality"
    assert feature["properties"]["region_name"] == "Amsterdam"
    assert feature["properties"]["source_version"] == "2025"
    assert feature["properties"]["valid_from"] == "2025-01-01"
    assert datetime.fromisoformat(feature["properties"]["retrieved_at"])


def test_archive_region_geom_skips_buitenland_feature(tmp_path, monkeypatch) -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object], url: str) -> None:
            self._payload = payload
            self.url = url
            self.links: dict[str, dict[str, str]] = {}

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeClient:
        def __init__(self, timeout: float) -> None:
            self.timeout = timeout

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, url: str) -> FakeResponse:
            return FakeResponse(_make_pdok_payload_with_buitenland(), url)

    fake_client = FakeClient(timeout=60.0)
    monkeypatch.setattr("fip.commands.gis.httpx.Client", lambda timeout: fake_client)

    result = runner.invoke(
        cli.app,
        [
            "archive-region-geom",
            "--year",
            "2025",
            "--run-id",
            "region-geom-2025",
            "--target",
            "local",
            "--output-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    output_file = (
        tmp_path
        / "raw"
        / "region_geom"
        / "cbs-wijken-en-buurten-2025"
        / "region-geom-2025"
        / "gemeenten.geojson"
    )
    payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert len(payload["features"]) == 1


def test_build_region_geom_command_parses_archived_geojson_and_writes_rows(
    tmp_path,
    monkeypatch,
) -> None:
    input_file = tmp_path / "region_geom.geojson"
    input_file.write_text(json.dumps(_make_archived_payload()), encoding="utf-8")

    calls: dict[str, object] = {}

    class FakeWriter:
        def __init__(self, table_name: str) -> None:
            calls["table_name"] = table_name

        def write(self, rows: list[dict[str, object]]) -> int:
            materialized = list(rows)
            calls["rows"] = materialized
            return len(materialized)

    monkeypatch.setattr("fip.commands.gis.RegionGeomLandingWriter", FakeWriter)

    result = runner.invoke(
        cli.app,
        [
            "build-region-geom",
            "--input",
            str(input_file),
            "--table",
            "region_geom",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 region geometry rows\n"
    assert calls["table_name"] == "region_geom"
    rows = cast(list[dict[str, object]], calls["rows"])
    assert len(rows) == 1
    row = rows[0]
    assert row["source_name"] == "pdok_cbs_wijken_en_buurten"
    assert row["natural_key"] == "GM0363|2025"
    assert row["run_id"] == "region-geom-2025"
    assert row["region_id"] == "GM0363"
    assert row["region_level"] == "municipality"
    assert row["region_name"] == "Amsterdam"
    assert row["source_version"] == "2025"
    assert row["retrieved_at"] == datetime.fromisoformat("2025-04-26T12:00:00+00:00")
    assert row["valid_from"] == date(2025, 1, 1)
    assert cast(str, row["geometry"]).startswith('{"coordinates"')


def test_load_region_geom_command_archives_and_loads(
    tmp_path,
    monkeypatch,
) -> None:
    archived_file = tmp_path / "region_geom.geojson"
    archived_file.write_text(json.dumps(_make_archived_payload()), encoding="utf-8")

    calls: dict[str, object] = {}

    def fake_archive_region_geom(**kwargs) -> str:
        calls["archive_kwargs"] = kwargs
        return str(archived_file)

    class FakeWriter:
        def __init__(self, table_name: str) -> None:
            calls["table_name"] = table_name

        def write(self, rows: list[dict[str, object]]) -> int:
            materialized = list(rows)
            calls["rows"] = materialized
            return len(materialized)

    monkeypatch.setattr("fip.commands.gis._archive_region_geom", fake_archive_region_geom)
    monkeypatch.setattr("fip.commands.gis.RegionGeomLandingWriter", FakeWriter)

    result = runner.invoke(
        cli.app,
        [
            "load-region-geom",
            "--year",
            "2025",
            "--run-id",
            "region-geom-2025",
            "--target",
            "local",
            "--output-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    archive_kwargs = cast(dict[str, object], calls["archive_kwargs"])
    assert archive_kwargs["target"] == "local"
    assert calls["table_name"] == "region_geom"
    rows = cast(list[dict[str, object]], calls["rows"])
    assert len(rows) == 1
    assert rows[0]["region_id"] == "GM0363"
