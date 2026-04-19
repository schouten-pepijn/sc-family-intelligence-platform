from typing import Protocol

from fip.lakehouse.silver.cbs_observations import flatten_bronze_observation_rows


class CBSSilverSink(Protocol):
    """Interface for CBS Silver layer sinks."""

    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_bronze_rows_to_cbs_observation_sink(
    bronze_rows: list[dict[str, object]],
    sink: CBSSilverSink,
) -> int:
    """Transform bronze rows to the CBS Silver schema and write to sink."""
    silver_rows = flatten_bronze_observation_rows(bronze_rows)
    return sink.write(silver_rows)
