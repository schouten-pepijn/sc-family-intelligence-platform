from typing import Protocol

from fip.ingestion.base import RawRecord


class Sink(Protocol):
    # Interface for data sink adapters
    def write(self, records: list[RawRecord]) -> int: ...
