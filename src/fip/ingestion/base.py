from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, Protocol


@dataclass(frozen=True)
class RawRecord:
    source_name: str
    entity_name: str
    natural_key: str
    retrieved_at: datetime
    run_id: str
    payload: dict
    schema_version: str
    http_status: int = 200


class Source(Protocol):
    name: str
    schema_version: str

    def iter_records(self, since: datetime | None = None) -> Iterator[RawRecord]:
        ...
    def healthcheck(self) -> bool:
        ...

