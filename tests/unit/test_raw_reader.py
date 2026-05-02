from __future__ import annotations

import json
from pathlib import Path

from fip.raw.reader import RawSnapshotReader


def test_raw_snapshot_reader_reads_bag_gpkg_records(tmp_path: Path) -> None:
    path = tmp_path / "raw" / "bag_gpkg" / "debug-gpkg" / "verblijfsobject.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        (
            '{"source_name": "bag_gpkg", "entity_name": "bag_gpkg.verblijfsobject", '
            '"natural_key": "0003010000126809", "retrieved_at": "2026-04-18T09:00:00+00:00", '
            '"run_id": "debug-gpkg", "schema_version": "v1", "http_status": 200, '
            '"payload": {"feature_id": 1, "identificatie": "0003010000126809"}}\n'
        ),
        encoding="utf-8",
    )

    reader = RawSnapshotReader(base_dir=tmp_path)
    records = list(reader.iter_bag_gpkg_records(run_id="debug-gpkg", layer="verblijfsobject"))

    assert len(records) == 1
    assert records[0].source_name == "bag_gpkg"
    assert records[0].entity_name == "bag_gpkg.verblijfsobject"
    assert records[0].natural_key == "0003010000126809"


def test_raw_snapshot_reader_reads_cbs_crime_extra_entities(tmp_path: Path) -> None:
    base = tmp_path / "raw" / "cbs" / "83648NED" / "crime-run"
    base.mkdir(parents=True)

    entities = (
        "Observations",
        "MeasureCodes",
        "PeriodenCodes",
        "RegioSCodes",
        "SoortMisdrijfCodes",
    )
    for entity in entities:
        record = {
            "source_name": "cbs_crime",
            "entity_name": f"83648NED.{entity}",
            "natural_key": entity,
            "retrieved_at": "2026-05-03T00:00:00+00:00",
            "run_id": "crime-run",
            "schema_version": "v1",
            "http_status": 200,
            "payload": {"Id": entity},
        }
        (base / f"{entity}.jsonl").write_text(json.dumps(record) + "\n", encoding="utf-8")

    reader = RawSnapshotReader(base_dir=tmp_path)

    records = list(reader.iter_cbs_records(table_id="83648NED", run_id="crime-run"))

    assert [record.entity_name for record in records] == [
        "83648NED.Observations",
        "83648NED.MeasureCodes",
        "83648NED.PeriodenCodes",
        "83648NED.RegioSCodes",
        "83648NED.SoortMisdrijfCodes",
    ]
