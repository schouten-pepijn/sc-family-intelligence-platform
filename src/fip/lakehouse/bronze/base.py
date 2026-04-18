from typing import Protocol

from fip.ingestion.base import RawRecord


class Sink(Protocol):
    def write(self, records: list[RawRecord]) -> int: ...


class SinkFactory(Protocol):
    def for_entity(self, entity_name: str) -> Sink: ...
