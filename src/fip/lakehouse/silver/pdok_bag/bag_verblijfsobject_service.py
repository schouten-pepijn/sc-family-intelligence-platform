from typing import Protocol

from fip.lakehouse.silver.pdok_bag.bag_verblijfsobject import (
    flatten_bronze_bag_verblijfsobject_rows,
)


class BAGSilverSink(Protocol):
    """Interface for BAG Silver layer sinks."""

    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_bronze_rows_to_bag_verblijfsobject_sink(
    bronze_rows: list[dict[str, object]],
    sink: BAGSilverSink,
) -> int:
    """Transform bronze rows to the BAG Silver schema and write to sink."""
    silver_rows = flatten_bronze_bag_verblijfsobject_rows(bronze_rows)
    return sink.write(silver_rows)
