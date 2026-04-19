from typing import Protocol

from fip.ingestion.base import RawRecord


class Sink(Protocol):
    """Interface for writers that persist records to storage systems."""
    def write(self, records: list[RawRecord]) -> int: ...


class SinkFactory(Protocol):
    """Factory for creating entity-specific sinks."""
    def for_entity(self, entity_name: str) -> Sink: ...
