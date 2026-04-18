from datetime import datetime, timezone

from fip.silver.observation_sink import SilverObservationSink


def make_silver_row(natural_key: str, observation_id: int) -> dict[str, object]:
    return {
        "source_name": "cbs_statline",
        "natural_key": natural_key,
        "retrieved_at": datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "observation_id": observation_id,
        "measure_code": "M001534",
        "period_code": "1995JJ00",
        "region_code": "NL01",
        "numeric_value": 93750.0,
        "value_attribute": "None",
        "string_value": None,
    }


def test_silver_observation_sink_initializes_with_table_ident() -> None:
    sink = SilverObservationSink(table_ident="silver.cbs_observations_flat_83625ned")

    assert sink.table_ident == "silver.cbs_observations_flat_83625ned"
    assert sink.last_written_rows == []


def test_silver_observation_sink_write_returns_number_of_rows() -> None:
    sink = SilverObservationSink(table_ident="silver.cbs_observations_flat_83625ned")
    rows = [make_silver_row("1", 1), make_silver_row("2", 2)]
    sink._load_catalog = lambda: object()  # type: ignore[method-assign]
    sink._ensure_table = lambda catalog, arrow_schema: object()  # type: ignore[method-assign]

    written = sink.write(rows)

    assert written == 2
    assert len(sink.last_written_rows) == 2
    assert sink.last_written_rows[0]["natural_key"] == "1"
    assert sink.last_written_rows[1]["observation_id"] == 2


def test_silver_observation_sink_write_returns_zero_for_empty_input() -> None:
    sink = SilverObservationSink(table_ident="silver.cbs_observations_flat_83625ned")

    written = sink.write([])

    assert written == 0
    assert sink.last_written_rows == []


def test_silver_observation_sink_to_arrow_table_uses_expected_schema() -> None:
    sink = SilverObservationSink(table_ident="silver.cbs_observations_flat_83625ned")
    rows = [make_silver_row("1", 1)]

    table = sink._to_arrow_table(rows)

    assert table.schema == sink._get_arrow_schema()


def test_silver_observation_sink_write_loads_catalog_and_ensures_table(
    monkeypatch,
) -> None:
    sink = SilverObservationSink(table_ident="silver.cbs_observations_flat_83625ned")
    rows = [make_silver_row("1", 1)]

    fake_catalog = object()
    calls: dict[str, object] = {}

    def fake_load_catalog() -> object:
        calls["load_catalog_called"] = True
        return fake_catalog

    def fake_ensure_table(catalog: object, arrow_schema) -> object:
        calls["catalog"] = catalog
        calls["arrow_schema"] = arrow_schema
        return object()

    monkeypatch.setattr(sink, "_load_catalog", fake_load_catalog)
    monkeypatch.setattr(sink, "_ensure_table", fake_ensure_table)

    written = sink.write(rows)

    assert written == 1
    assert calls["load_catalog_called"] is True
    assert calls["catalog"] is fake_catalog
    assert calls["arrow_schema"] == sink._get_arrow_schema()
