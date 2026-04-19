from abc import ABC, abstractmethod

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from fip.settings import get_settings


class SilverIcebergSink(ABC):
    """Shared Iceberg sink implementation for Silver tables."""

    def __init__(self, table_ident: str) -> None:
        self.table_ident = table_ident
        self.last_written_rows: list[dict[str, object]] = []

    def write(self, rows: list[dict[str, object]]) -> int:
        if not rows:
            self.last_written_rows = []
            return 0

        self.last_written_rows = [self._to_silver_row(row) for row in rows]
        arrow_table = self._to_arrow_table(self.last_written_rows)
        catalog = self._load_catalog()
        table = self._replace_table(catalog, arrow_table.schema)

        first_row = self.last_written_rows[0]
        snapshot_properties: dict[str, str] = {
            "fip.source_name": str(first_row["source_name"]),
            "fip.run_id": str(first_row["run_id"]),
            "fip.schema_version": str(first_row["schema_version"]),
        }
        table.append(
            arrow_table,
            snapshot_properties=snapshot_properties,
        )
        return arrow_table.num_rows

    @abstractmethod
    def _to_silver_row(self, row: dict[str, object]) -> dict[str, object]: ...

    @abstractmethod
    def _get_arrow_schema(self) -> pa.Schema: ...

    def _to_arrow_table(self, rows: list[dict[str, object]]) -> pa.Table:
        return pa.Table.from_pylist(rows, schema=self._get_arrow_schema())

    def _namespace(self) -> str:
        return self.table_ident.split(".", maxsplit=1)[0]

    def _load_catalog(self) -> Catalog:
        settings = get_settings()
        properties: dict[str, str] = {
            "type": "rest",
            "uri": settings.lakekeeper_catalog_uri,
            "warehouse": settings.lakekeeper_warehouse_name,
            "s3.endpoint": settings.s3_endpoint,
            "s3.access-key-id": settings.s3_access_key_id,
            "s3.secret-access-key": settings.s3_secret_access_key,
            "s3.region": settings.aws_region,
            "s3.force-virtual-addressing": str(not settings.s3_path_style_access).lower(),
        }
        return load_catalog("lakekeeper", **properties)

    def _ensure_namespace(self, catalog: Catalog) -> None:
        catalog.create_namespace_if_not_exists(self._namespace())

    def _replace_table(self, catalog: Catalog, arrow_schema: pa.Schema) -> Table:
        # Drop-then-create allows atomic schema updates when payload fields change;
        # NoSuchTableError on first run is expected and handled gracefully.
        self._ensure_namespace(catalog)

        try:
            catalog.drop_table(self.table_ident)
        except NoSuchTableError:
            pass

        return catalog.create_table_if_not_exists(
            self.table_ident,
            schema=arrow_schema,
        )
