from typing import Protocol

from fip.lakehouse.silver.cbs_observations import flatten_bronze_observation_rows


class SilverSink(Protocol):
    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_bronze_rows_to_silver_sink(
    bronze_rows: list[dict[str, object]],
    sink: SilverSink,
) -> int:
    silver_rows = flatten_bronze_observation_rows(bronze_rows)
    return sink.write(silver_rows)
