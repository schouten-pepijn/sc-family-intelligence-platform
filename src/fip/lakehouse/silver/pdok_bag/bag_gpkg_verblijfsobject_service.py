from typing import Protocol

from fip.lakehouse.silver.pdok_bag.bag_gpkg_verblijfsobject import (
    flatten_bronze_bag_gpkg_verblijfsobject_rows,
)


class BAGGpkgSilverSink(Protocol):
    """Interface for BAG GPKG Silver layer sinks."""

    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_bronze_rows_to_bag_gpkg_verblijfsobject_sink(
    bronze_rows: list[dict[str, object]],
    sink: BAGGpkgSilverSink,
) -> int:
    """Transform bronze rows to the BAG GPKG Silver schema and write to sink."""
    silver_rows = flatten_bronze_bag_gpkg_verblijfsobject_rows(bronze_rows)
    return sink.write(silver_rows)

