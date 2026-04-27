from __future__ import annotations

from pathlib import Path

from fip.raw.reader import RawSnapshotReader


def test_raw_snapshot_reader_reads_bag_gpkg_records(tmp_path: Path) -> None:
    path = tmp_path / "raw" / "bag_gpkg" / "debug-gpkg" / "verblijfsobject.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        (
            '{"source_name": "bag_gpkg", "entity_name": "bag.verblijfsobject", '
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
    assert records[0].entity_name == "bag.verblijfsobject"
    assert records[0].natural_key == "0003010000126809"
