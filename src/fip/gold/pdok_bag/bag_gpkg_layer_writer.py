from __future__ import annotations

from collections.abc import Sequence

from fip.gold.core.postgres import PostgresFullRefreshWriter
from fip.lakehouse.silver.pdok_bag.bag_gpkg_layers import BAG_GPKG_LAYER_CONFIGS


class BAGGpkgLayerLandingWriter(PostgresFullRefreshWriter):
    """Writes BAG GPKG layer rows to Postgres in the landing layer."""

    def __init__(self, layer: str, table_name: str) -> None:
        if layer not in BAG_GPKG_LAYER_CONFIGS:
            supported = ", ".join(sorted(BAG_GPKG_LAYER_CONFIGS))
            raise ValueError(
                f"Unsupported BAG GPKG landing layer '{layer}'. Expected one of: {supported}"
            )
        super().__init__(table_name=table_name)
        self.layer = layer

    def _to_row(self, row: object) -> dict[str, object]:
        mapping = row
        return {field: mapping[field] for field in self._field_names()}  # type: ignore[index]

    def _field_names(self) -> Sequence[str]:
        return BAG_GPKG_LAYER_CONFIGS[self.layer].fields

    def _table_columns_sql(self) -> str:
        return ",\n".join(
            f"                    {field} {self._field_sql_type(field)}"
            for field in self._field_names()
        )

    def _field_sql_type(self, field: str) -> str:
        if field == "retrieved_at":
            return "timestamptz NOT NULL"
        if field == "http_status":
            return "integer NOT NULL"
        if field in BAG_GPKG_LAYER_CONFIGS[self.layer].int_columns:
            return "bigint"
        if field in {
            "source_name",
            "natural_key",
            "run_id",
            "schema_version",
            "bag_id",
            f"{self.layer}_identificatie",
        }:
            return "text NOT NULL"
        return "text"
