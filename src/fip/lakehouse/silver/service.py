from typing import Protocol

from fip.lakehouse.silver.cbs_observations import flatten_bronze_observation_rows


class SilverSink(Protocol):
    """Interface for Silver layer sinks."""
    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_bronze_rows_to_silver_sink(
    bronze_rows: list[dict[str, object]],
    sink: SilverSink,
) -> int:
    """Transform bronze rows to Silver schema and write to sink.

    Flattening extracts payload fields and renames them per domain terms,
    decoupling Silver consumers from Bronze payload structure changes.
    """
    silver_rows = flatten_bronze_observation_rows(bronze_rows)
    return sink.write(silver_rows)
