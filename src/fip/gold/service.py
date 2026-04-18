from typing import Protocol


class GoldSink(Protocol):
    def write(self, rows: list[dict[str, object]]) -> int: ...


def write_silver_rows_to_gold_sink(
    silver_rows: list[dict[str, object]],
    sink: GoldSink,
) -> int:
    return sink.write(silver_rows)
