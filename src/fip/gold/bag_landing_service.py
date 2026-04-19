from typing import Protocol


class BAGLandingSink(Protocol):
    """Interface for BAG landing layer sinks."""

    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_silver_rows_to_bag_landing_sink(
    silver_rows: list[dict[str, object]],
    sink: BAGLandingSink,
) -> int:
    """Write BAG Silver rows to a BAG landing sink."""
    return sink.write(silver_rows)
