import io
from datetime import datetime, timezone
from pathlib import Path

from fip.ingestion.base import RawRecord
from fip.raw.writer import MinioRawSnapshotWriter, RawSnapshotWriter, serialize_raw_record


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


def test_minio_raw_snapshot_writer_writes_jsonl(monkeypatch) -> None:
    class FakeFile:
        def __init__(self, buffer: io.StringIO) -> None:
            self.buffer = buffer

        def __enter__(self) -> io.StringIO:
            return self.buffer

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeS3FileSystem:
        def __init__(self, *args, **kwargs) -> None:
            self.paths: list[str] = []
            self.buffers: dict[str, io.StringIO] = {}

        def open(self, path: str, mode: str, encoding: str = "utf-8") -> FakeFile:
            self.paths.append(path)
            buffer = io.StringIO()
            self.buffers[path] = buffer
            return FakeFile(buffer)

    monkeypatch.setattr("fip.raw.writer.s3fs.S3FileSystem", FakeS3FileSystem)

    writer = MinioRawSnapshotWriter(bucket="fip-lakehouse")
    records = [
        make_record("83625NED.MeasureCodes", "M001534", {"Identifier": "M001534", "Title": "A"}),
        make_record("83625NED.MeasureCodes", "M001535", {"Identifier": "M001535", "Title": "B"}),
    ]

    written = writer.write(records)

    assert written == 2
    path = "s3://fip-lakehouse/raw/cbs/83625NED/debug-raw/MeasureCodes.jsonl"
    assert writer.filesystem.paths == [path]
    assert writer.filesystem.buffers[path].getvalue().count("\n") == 2
