import pyarrow as pa

from fip.lakehouse.silver.core.sink import SilverIcebergSink
from fip.lakehouse.silver.pdok_bag.bag_gpkg_layers import (
    BAG_GPKG_LAYER_CONFIGS,
    to_bag_gpkg_layer_row,
)


class BAGGpkgLayerSink(SilverIcebergSink):
    """Writes BAG GPKG layer rows to Iceberg tables in Silver."""

    def __init__(self, layer: str, table_ident: str) -> None:
        if layer not in BAG_GPKG_LAYER_CONFIGS:
            supported = ", ".join(sorted(BAG_GPKG_LAYER_CONFIGS))
            raise ValueError(
                f"Unsupported BAG GPKG Silver layer '{layer}'. Expected one of: {supported}"
            )
        super().__init__(table_ident=table_ident)
        self.layer = layer

    def _to_silver_row(self, row: dict[str, object]) -> dict[str, object]:
        return to_bag_gpkg_layer_row(self.layer, row)

    def _get_arrow_schema(self) -> pa.Schema:
        return pa.schema([self._field_to_arrow_field(field) for field in self._fields()])

    def _fields(self) -> tuple[str, ...]:
        return BAG_GPKG_LAYER_CONFIGS[self.layer].fields

    def _field_to_arrow_field(self, field: str) -> pa.Field:
        if field == "retrieved_at":
            return pa.field(field, pa.timestamp("us", tz="UTC"), nullable=False)
        if field == "http_status":
            return pa.field(field, pa.int32(), nullable=False)
        if field in BAG_GPKG_LAYER_CONFIGS[self.layer].int_columns:
            return pa.field(field, pa.int64(), nullable=True)
        nullable = field not in {
            "source_name",
            "natural_key",
            "retrieved_at",
            "run_id",
            "schema_version",
            "http_status",
            "bag_id",
            f"{self.layer}_identificatie",
        }
        return pa.field(field, pa.string(), nullable=nullable)
