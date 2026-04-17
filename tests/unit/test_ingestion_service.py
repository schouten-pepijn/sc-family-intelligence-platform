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
        self.batches: list[list[RawRecord]] = []

    def write(self, records: list[RawRecord]) -> int:
        self.batches.append(records)
        self.received = records
        return len(records)


class FakeSinkFactory:
    def __init__(self) -> None:
        self.sinks: dict[str, FakeSink] = {}

    def for_entity(self, entity_name: str) -> FakeSink:
        if entity_name not in self.sinks:
            self.sinks[entity_name] = FakeSink()
        return self.sinks[entity_name]


def test_ingest_source_to_sink_writes_records_and_returns_count() -> None:
    source = FakeSource()
    sink_factory = FakeSinkFactory()

    written = ingest_source_to_sink(source, sink_factory)
    sink = sink_factory.sinks["fake.entity"]

    assert written == 1
    assert len(sink.received) == 1
    assert len(sink.batches) == 1
    assert sink.received[0].source_name == "fake_source"
    assert sink.received[0].entity_name == "fake.entity"
    assert sink.received[0].natural_key == "1"


class FakeMultiEntitySource:
    name = "fake_source"
    schema_version = "v1"

    def iter_records(self, since=None):
        yield RawRecord(
            source_name="fake_source",
            entity_name="fake.observations",
            natural_key="1",
            retrieved_at=datetime.now(timezone.utc),
            run_id="run-001",
            payload={"Id": 1},
            schema_version="v1",
        )
        yield RawRecord(
            source_name="fake_source",
            entity_name="fake.measure_codes",
            natural_key="2",
            retrieved_at=datetime.now(timezone.utc),
            run_id="run-001",
            payload={"Id": 2},
            schema_version="v1",
        )
        yield RawRecord(
            source_name="fake_source",
            entity_name="fake.observations",
            natural_key="3",
            retrieved_at=datetime.now(timezone.utc),
            run_id="run-001",
            payload={"Id": 3},
            schema_version="v1",
        )

    def healthcheck(self) -> bool:
        return True


def test_ingest_source_to_sink_groups_records_by_entity_name() -> None:
    source = FakeMultiEntitySource()
    sink_factory = FakeSinkFactory()

    written = ingest_source_to_sink(source, sink_factory)

    assert written == 3
    assert set(sink_factory.sinks) == {"fake.observations", "fake.measure_codes"}

    observation_sink = sink_factory.sinks["fake.observations"]
    measure_codes_sink = sink_factory.sinks["fake.measure_codes"]

    assert len(observation_sink.batches) == 1
    assert len(measure_codes_sink.batches) == 1

    assert len(observation_sink.received) == 2
    assert len(measure_codes_sink.received) == 1

    assert {record.entity_name for record in observation_sink.received} == {"fake.observations"}
    assert {record.entity_name for record in measure_codes_sink.received} == {"fake.measure_codes"}
