from typing import Protocol

from fip.lakehouse.silver.pdok_bag.bag_adressen import flatten_bronze_bag_adressen_rows


class BAGAdressenSilverSink(Protocol):
    """Interface for BAG adres Silver layer sinks."""

    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_bronze_rows_to_bag_adressen_sink(
    bronze_rows: list[dict[str, object]],
    sink: BAGAdressenSilverSink,
) -> int:
    """Transform bronze BAG adres rows to the Silver schema and write to sink."""
    silver_rows = flatten_bronze_bag_adressen_rows(bronze_rows)
    return sink.write(silver_rows)
