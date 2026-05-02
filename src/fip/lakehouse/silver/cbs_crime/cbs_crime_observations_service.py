from __future__ import annotations

from typing import Protocol

from fip.lakehouse.silver.cbs_crime.cbs_crime_observations import (
    flatten_bronze_crime_observation_rows,
)


class CBSCrimeSilverSink(Protocol):
    """Interface for CBS crime Silver layer sinks."""

    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_bronze_rows_to_cbs_crime_observation_sink(
    bronze_rows: list[dict[str, object]],
    sink: CBSCrimeSilverSink,
) -> int:
    """Transform CBS crime Bronze rows to the Silver schema and write to sink."""
    silver_rows = flatten_bronze_crime_observation_rows(bronze_rows)
    if not silver_rows:
        return 0

    return sink.write(silver_rows)
