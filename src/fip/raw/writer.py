from __future__ import annotations

import json
from pathlib import Path

from fip.ingestion.base import RawRecord


def serialize_raw_record(record: RawRecord) -> str:
    return json.dumps(
        {
            "source_name": record.source_name,
            "entity_name": record.entity_name,
            "natural_key": record.natural_key,
            "retrieved_at": record.retrieved_at.isoformat(),
            "run_id": record.run_id,
            "schema_version": record.schema_version,
            "http_status": record.http_status,
            "payload": record.payload,
        },
        ensure_ascii=False,
    )


class RawSnapshotWriter:
    def __init__(self, base_dir: Path | str) -> None:
        self.base_dir = Path(base_dir)

    def write(self, records: list[RawRecord]) -> int:
        if not records:
            return 0

        self._validate_single_entity(records)
        path = self._snapshot_path(records[0])
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(serialize_raw_record(record))
                handle.write("\n")

        return len(records)

    def _snapshot_path(self, record: RawRecord) -> Path:
        table_id, entity = record.entity_name.split(".", maxsplit=1)
        return self.base_dir / "raw" / "cbs" / table_id / record.run_id / f"{entity}.jsonl"

    def _validate_single_entity(self, records: list[RawRecord]) -> None:
        first_entity = records[0].entity_name
        if any(record.entity_name != first_entity for record in records):
            raise ValueError("RawSnapshotWriter.write expects records for a single entity")
