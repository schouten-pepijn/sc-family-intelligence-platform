from datetime import datetime, timezone
from pathlib import Path

from fip.ingestion.base import RawRecord
from fip.raw.writer import RawSnapshotWriter, serialize_raw_record


def make_record(entity_name: str, natural_key: str, payload: dict[str, object]) -> RawRecord:
    return RawRecord(
        source_name="cbs_statline",
        entity_name=entity_name,
        natural_key=natural_key,
        retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        run_id="debug-raw",
        payload=payload,
        schema_version="v1",
        http_status=200,
    )


def test_raw_snapshot_writer_writes_jsonl(tmp_path: Path) -> None:
    writer = RawSnapshotWriter(base_dir=tmp_path)
    records = [
        make_record("83625NED.MeasureCodes", "M001534", {"Identifier": "M001534", "Title": "A"}),
        make_record("83625NED.MeasureCodes", "M001535", {"Identifier": "M001535", "Title": "B"}),
    ]

    written = writer.write(records)

    assert written == 2
    path = tmp_path / "raw" / "cbs" / "83625NED" / "debug-raw" / "MeasureCodes.jsonl"
    assert path.exists()
    assert path.read_text(encoding="utf-8").count("\n") == 2


def test_serialize_raw_record_returns_json_payload() -> None:
    record = make_record(
        "83625NED.MeasureCodes",
        "M001534",
        {"Identifier": "M001534", "Title": "A"},
    )

    line = serialize_raw_record(record)

    assert '"entity_name": "83625NED.MeasureCodes"' in line
    assert '"natural_key": "M001534"' in line
    assert '"payload": {' in line
