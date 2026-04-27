from typing import Protocol

from fip.lakehouse.silver.pdok_bag.bag_gpkg_layers import (
    flatten_bronze_bag_gpkg_layer_rows,
)


class BAGGpkgLayerSilverSink(Protocol):
    """Interface for BAG GPKG layer Silver sinks."""

    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_bronze_rows_to_bag_gpkg_layer_sink(
    layer: str,
    bronze_rows: list[dict[str, object]],
    sink: BAGGpkgLayerSilverSink,
) -> int:
    """Transform bronze BAG GPKG layer rows to the Silver schema and write to sink."""
    silver_rows = flatten_bronze_bag_gpkg_layer_rows(layer, bronze_rows)
    return sink.write(silver_rows)
