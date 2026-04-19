from typing import Protocol

from fip.lakehouse.silver.pdok_bag.bag_pand import flatten_bronze_bag_pand_rows


class BAGPandSilverSink(Protocol):
    """Interface for BAG pand Silver layer sinks."""

    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_bronze_rows_to_bag_pand_sink(
    bronze_rows: list[dict[str, object]],
    sink: BAGPandSilverSink,
) -> int:
    """Transform bronze BAG pand rows to the Silver schema and write to sink."""
    silver_rows = flatten_bronze_bag_pand_rows(bronze_rows)
    return sink.write(silver_rows)
