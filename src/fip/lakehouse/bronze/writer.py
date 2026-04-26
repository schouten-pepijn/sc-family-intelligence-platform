import json

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table

from fip.ingestion.base import RawRecord
from fip.lakehouse.config import configure_table_io_for_host, iceberg_catalog_properties
from fip.settings import get_settings

BRONZE_ROW_FIELDS = (
    "source_name",
    "entity_name",
    "natural_key",
    "retrieved_at",
    "run_id",
    "schema_version",
    "http_status",
    "payload",
)


class IcebergSink:
    """Writes raw records to Iceberg tables in the Bronze layer.

    Stores raw payloads as JSON strings to preserve source structure,
    enabling independent schema versioning and auditing of raw data.
    """

    def __init__(self, table_ident: str) -> None:
        self.table_ident = table_ident
        self.last_written: list[RawRecord] = []
        self.last_written_rows: list[dict[str, object]] = []

    def write(self, records: list[RawRecord]) -> int:
        """Write records to Iceberg, validating single entity per batch."""
        if records:
            first_entity = records[0].entity_name
            if any(record.entity_name != first_entity for record in records):
                raise ValueError("IcebergSink.write expects records for a single entity")

        self.last_written = list(records)
        self.last_written_rows = [self._serialize_record(record) for record in records]

        if not records:
            return 0

        arrow_table = self._to_arrow_table(self.last_written_rows)
        catalog = self._load_catalog()
        table = self._ensure_table(catalog, arrow_table.schema)
        configure_table_io_for_host(table, get_settings())
        table.append(
            arrow_table,
            snapshot_properties={
                "fip.run_id": records[0].run_id,
                "fip.entity_name": records[0].entity_name,
                "fip.source_name": records[0].source_name,
            },
        )

        return len(records)

    def _serialize_record(self, record: RawRecord) -> dict[str, object]:
        # Payload is JSON-serialized to preserve original source structure;
        # sorting keys for deterministic snapshots and auditing purposes.
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

    def _to_arrow_table(self, rows: list[dict[str, object]]) -> pa.Table:
        return pa.Table.from_pylist(rows, schema=self._get_arrow_schema())

    def _get_arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("source_name", pa.string()),
                pa.field("entity_name", pa.string()),
                pa.field("natural_key", pa.string()),
                pa.field("retrieved_at", pa.timestamp("ms")),
                pa.field("run_id", pa.string()),
                pa.field("schema_version", pa.string()),
                pa.field("http_status", pa.int32()),
                pa.field("payload", pa.string()),
            ]
        )

    def _namespace(self) -> str:
        return self.table_ident.split(".", maxsplit=1)[0]

    def _load_catalog(self) -> Catalog:
        # Catalog is loaded fresh per write to handle credential rotation and
        # allow independent connection lifecycle management per sink instance.
        settings = get_settings()

        return load_catalog("polaris", **iceberg_catalog_properties(settings))

    def _ensure_namespace(self, catalog: Catalog) -> None:
        catalog.create_namespace_if_not_exists(self._namespace())

    def _ensure_table(self, catalog: Catalog, arrow_schema: pa.Schema) -> Table:
        self._ensure_namespace(catalog)
        return catalog.create_table_if_not_exists(
            self.table_ident,
            schema=arrow_schema,
        )
