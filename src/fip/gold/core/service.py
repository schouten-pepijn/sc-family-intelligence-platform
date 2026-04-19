from typing import Protocol


class PostgresSink(Protocol):
    """Interface for Postgres landing sinks."""

    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_rows_to_sink(
    rows: list[dict[str, object]],
    sink: PostgresSink,
) -> int:
    """Write rows to a Postgres sink."""
    return sink.write(rows)
