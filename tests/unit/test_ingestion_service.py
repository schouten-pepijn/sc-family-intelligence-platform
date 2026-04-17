from datetime import datetime, timezone

from fip.ingestion.base import RawRecord
from fip.ingestion.service import ingest_source_to_sink


class FakeSource:
    name = "fake_source"
    schema_version = "v1"

    def iter_records(self, since=None):
        yield RawRecord(
            source_name="fake_source",
            entity_name="fake.entity",
            natural_key="1",
            retrieved_at=datetime.now(timezone.utc),
            run_id="run-001",
            payload={"Id": 1},
            schema_version="v1",
        )

    def healthcheck(self) -> bool:
        return True


class FakeSink:
    def __init__(self) -> None:
        self.received: list[RawRecord] = []

    def write(self, records: list[RawRecord]) -> int:
        self.received = records
        return len(records)


def test_ingest_source_to_sink_writes_records_and_returns_count() -> None:
    source = FakeSource()
    sink = FakeSink()

    written = ingest_source_to_sink(source, sink)

    assert written == 1
    assert len(sink.received) == 1
    assert sink.received[0].source_name == "fake_source"
    assert sink.received[0].entity_name == "fake.entity"
    assert sink.received[0].natural_key == "1"
