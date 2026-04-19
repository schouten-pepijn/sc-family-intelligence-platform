from datetime import datetime, timezone

from fip.lakehouse.silver.cbs.cbs_observations_service import (
    write_bronze_rows_to_cbs_observation_sink,
)


def make_bronze_row(natural_key: str, observation_id: int) -> dict[str, object]:
    return {
        "source_name": "cbs_statline",
        "natural_key": natural_key,
        "retrieved_at": datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "payload": (
            '{"Id": '
            f"{observation_id}, "
            '"Measure": "M001534", "Perioden": "1995JJ00", '
            '"RegioS": "NL01", "StringValue": null, "Value": 93750.0, '
            '"ValueAttribute": "None"}'
        ),
    }


class FakeSilverSink:
    def __init__(self) -> None:
        self.written_rows: list[dict[str, object]] = []

    def write(self, rows: list[dict[str, object]]) -> int:
        self.written_rows = rows
        return len(rows)


def test_write_bronze_rows_to_cbs_observation_sink_flattens_and_writes_rows() -> None:
    sink = FakeSilverSink()
    bronze_rows = [make_bronze_row("1", 1), make_bronze_row("2", 2)]

    written = write_bronze_rows_to_cbs_observation_sink(bronze_rows, sink)

    assert written == 2
    assert len(sink.written_rows) == 2
    assert sink.written_rows[0]["observation_id"] == 1
    assert sink.written_rows[1]["observation_id"] == 2
