from typing import Protocol

from fip.ingestion.base import RawRecord


class Sink(Protocol):
    # Interface for data sink adapters
    def write(self, records: list[RawRecord]) -> int: ...


class SinkFactory(Protocol):
    # Factory for creating Sink instances
    def for_entity(self, entity_name: str) -> Sink: ...
