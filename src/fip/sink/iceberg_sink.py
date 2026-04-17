from fip.ingestion.base import RawRecord


class IcebergSink:
    def __init__(self, table_ident: str) -> None:
        self.table_ident = table_ident

    def write(self, records: list[RawRecord]) -> int:
        return len(records)
