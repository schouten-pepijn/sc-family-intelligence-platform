from fip.ingestion.base import RawRecord


class IcebergSink:
    def __init__(self, table_ident: str) -> None:
        self.table_ident = table_ident
        self.last_written: list[RawRecord] = []

    def write(self, records: list[RawRecord]) -> int:
        if records:
            first_entity = records[0].entity_name
            if any(record.entity_name != first_entity for record in records):
                raise ValueError("IcebergSink.write expects records for a single entity")

        self.last_written = records
        return len(records)
