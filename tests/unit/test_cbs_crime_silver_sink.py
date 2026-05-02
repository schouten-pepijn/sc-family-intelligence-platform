from __future__ import annotations

from datetime import datetime, timezone
from typing import cast

from pyiceberg.catalog import Catalog

from fip.lakehouse.silver.cbs_crime.cbs_crime_observations_sink import (
    CBSCrimeObservationSink,
)


def make_silver_row(natural_key: str, observation_id: str) -> dict[str, object]:
    return {
        "source_name": "cbs_crime",
        "entity_name": "83648NED.Observations",
        "natural_key": natural_key,
        "retrieved_at": datetime(2026, 5, 3, tzinfo=timezone.utc),
        "run_id": "crime-run",
        "schema_version": "v1",
        "http_status": 200,
        "observation_id": observation_id,
        "measure_id": "M004200_2",
        "crime_type_id": "1.1",
        "region_id": "GM0363",
        "period_id": "2024JJ00",
        "period_year": 2024,
        "value": 123.0,
        "value_attribute": None,
    }


def test_cbs_crime_observation_sink_initializes_with_table_ident() -> None:
    sink = CBSCrimeObservationSink(table_ident="silver.cbs_crime_observations_flat_83648ned")

    assert sink.table_ident == "silver.cbs_crime_observations_flat_83648ned"
    assert sink.last_written_rows == []


def test_cbs_crime_observation_sink_write_returns_zero_for_empty_input() -> None:
    sink = CBSCrimeObservationSink(table_ident="silver.cbs_crime_observations_flat_83648ned")

    written = sink.write([])

    assert written == 0
    assert sink.last_written_rows == []


def test_cbs_crime_observation_sink_to_arrow_table_uses_expected_schema() -> None:
    sink = CBSCrimeObservationSink(table_ident="silver.cbs_crime_observations_flat_83648ned")
    rows = [make_silver_row("1", "1")]

    table = sink._to_arrow_table(rows)

    assert table.schema == sink._get_arrow_schema()


def test_cbs_crime_observation_sink_write_replaces_table_and_appends_with_snapshot_properties(
    monkeypatch,
) -> None:
    sink = CBSCrimeObservationSink(table_ident="silver.cbs_crime_observations_flat_83648ned")
    rows = [make_silver_row("1", "1")]

    fake_catalog = object()
    calls: dict[str, object] = {}

    class FakeTable:
        def append(self, df, snapshot_properties=None, branch=None) -> None:
            calls["df"] = df
            calls["snapshot_properties"] = snapshot_properties
            calls["branch"] = branch

    def fake_load_catalog() -> object:
        calls["load_catalog_called"] = True
        return fake_catalog

    def fake_replace_table(catalog: object, arrow_schema) -> FakeTable:
        calls["catalog"] = catalog
        calls["arrow_schema"] = arrow_schema
        return FakeTable()

    monkeypatch.setattr(sink, "_load_catalog", fake_load_catalog)
    monkeypatch.setattr(sink, "_replace_table", fake_replace_table)

    written = sink.write(rows)

    assert written == 1
    assert calls["load_catalog_called"] is True
    assert calls["catalog"] is fake_catalog
    assert calls["arrow_schema"] == sink._get_arrow_schema()
    assert calls["snapshot_properties"] == {
        "fip.source_name": "cbs_crime",
        "fip.run_id": "crime-run",
        "fip.schema_version": "v1",
    }
    assert calls["branch"] is None


def test_cbs_crime_observation_sink_replace_table_drops_existing_table_before_recreating() -> None:
    sink = CBSCrimeObservationSink(table_ident="silver.cbs_crime_observations_flat_83648ned")
    calls: list[tuple[str, object]] = []

    class FakeCatalog:
        def create_namespace_if_not_exists(self, namespace: str) -> None:
            calls.append(("namespace", namespace))

        def drop_table(self, identifier: str) -> None:
            calls.append(("drop", identifier))

        def create_table_if_not_exists(self, identifier: str, schema) -> str:
            calls.append(("create", identifier))
            return "fake-table"

    table = sink._replace_table(cast(Catalog, FakeCatalog()), sink._get_arrow_schema())

    assert table == "fake-table"
    assert calls == [
        ("namespace", "silver"),
        ("drop", "silver.cbs_crime_observations_flat_83648ned"),
        ("create", "silver.cbs_crime_observations_flat_83648ned"),
    ]
