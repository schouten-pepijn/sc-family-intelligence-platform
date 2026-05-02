from __future__ import annotations

from datetime import datetime, timezone

from fip.lakehouse.silver.cbs_crime.cbs_crime_observations_service import (
    write_bronze_rows_to_cbs_crime_observation_sink,
)


def make_bronze_row(
    natural_key: str,
    observation_id: int,
    entity_name: str = "83648NED.Observations",
) -> dict[str, object]:
    return {
        "source_name": "cbs_crime",
        "entity_name": entity_name,
        "natural_key": natural_key,
        "retrieved_at": datetime(2026, 5, 3, tzinfo=timezone.utc),
        "run_id": "crime-run",
        "schema_version": "v1",
        "http_status": 200,
        "payload": (
            '{"Id": '
            f"{observation_id}, "
            '"Measure": "M004200_2", "SoortMisdrijf": "1.1", '
            '"RegioS": "GM0363", "Perioden": "2024JJ00", '
            '"Value": 123.0, "ValueAttribute": null}'
        ),
    }


class FakeSilverSink:
    def __init__(self) -> None:
        self.written_rows: list[dict[str, object]] = []

    def write(self, rows: list[dict[str, object]]) -> int:
        self.written_rows = rows
        return len(rows)


def test_write_bronze_rows_to_cbs_crime_observation_sink_flattens_and_writes_rows() -> None:
    sink = FakeSilverSink()
    bronze_rows = [make_bronze_row("1", 1), make_bronze_row("2", 2)]

    written = write_bronze_rows_to_cbs_crime_observation_sink(bronze_rows, sink)

    assert written == 2
    assert len(sink.written_rows) == 2
    assert sink.written_rows[0]["observation_id"] == "1"
    assert sink.written_rows[1]["observation_id"] == "2"


def test_write_bronze_rows_to_cbs_crime_observation_sink_filters_non_observation_rows() -> None:
    sink = FakeSilverSink()
    bronze_rows = [
        make_bronze_row("1", 1),
        make_bronze_row("crime-type", 1, entity_name="83648NED.SoortMisdrijfCodes"),
    ]

    written = write_bronze_rows_to_cbs_crime_observation_sink(bronze_rows, sink)

    assert written == 1
    assert len(sink.written_rows) == 1
    assert sink.written_rows[0]["entity_name"] == "83648NED.Observations"


def test_write_bronze_rows_to_cbs_crime_observation_sink_returns_zero_for_no_observations() -> None:
    sink = FakeSilverSink()
    bronze_rows = [
        make_bronze_row("crime-type", 1, entity_name="83648NED.SoortMisdrijfCodes"),
    ]

    written = write_bronze_rows_to_cbs_crime_observation_sink(bronze_rows, sink)

    assert written == 0
    assert sink.written_rows == []
