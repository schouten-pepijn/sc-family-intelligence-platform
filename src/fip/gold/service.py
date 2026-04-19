from typing import Protocol


class GoldSink(Protocol):
    """Interface for Gold layer sinks."""

    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_silver_rows_to_gold_sink(
    silver_rows: list[dict[str, object]],
    sink: GoldSink,
) -> int:
    """Write Silver rows to Gold sink.

    No transformation needed; Silver schema already matches Gold requirements.
    """
    return sink.write(silver_rows)
