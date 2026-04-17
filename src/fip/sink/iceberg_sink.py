import json

from fip.ingestion.base import RawRecord


class IcebergSink:
    def __init__(self, table_ident: str) -> None:
        self.table_ident = table_ident
        self.last_written: list[RawRecord] = []
        self.last_written_rows: list[dict[str, object]] = []

    def write(self, records: list[RawRecord]) -> int:
        if records:
            first_entity = records[0].entity_name
            if any(record.entity_name != first_entity for record in records):
                raise ValueError("IcebergSink.write expects records for a single entity")

        self.last_written = list(records)
        self.last_written_rows = [self._serialize_record(record) for record in records]
        return len(records)

    def _serialize_record(self, record: RawRecord) -> dict[str, object]:
        return {
            "source_name": record.source_name,
            "entity_name": record.entity_name,
            "natural_key": record.natural_key,
            "retrieved_at": record.retrieved_at,
            "run_id": record.run_id,
            "schema_version": record.schema_version,
            "http_status": record.http_status,
            "payload": json.dumps(record.payload, sort_keys=True),
        }
