import json
from datetime import datetime, timezone

from fip.lakehouse.silver.pdok_bag.bag_pand import (
    BAG_PAND_FIELDS,
    flatten_bronze_bag_pand,
    flatten_bronze_bag_pand_rows,
    to_bag_pand_row,
)
from fip.lakehouse.silver.pdok_bag.bag_pand_sink import BAGPandSink


def make_bag_pand_bronze_row(natural_key: str, bag_id: str) -> dict[str, object]:
    return {
        "source_name": "bag_pdok",
        "natural_key": natural_key,
        "retrieved_at": datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "payload": json.dumps(
            {
                "type": "Feature",
                "id": bag_id,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[5.86, 51.0], [5.87, 51.0], [5.87, 51.01], [5.86, 51.0]]],
                },
                "properties": {
                    "identificatie": "1960100000000001",
                    "status": "Pand in gebruik",
                    "oorspronkelijk_bouwjaar": 1974,
                    "geconstateerd": "N",
                    "documentdatum": "2023-03-01",
                    "documentnummer": "doc-123",
                },
            },
            ensure_ascii=False,
        ),
    }


def test_flatten_bronze_bag_pand_maps_payload_fields_to_silver_columns() -> None:
    row = make_bag_pand_bronze_row("1", "4c396a25-0e16-586f-a298-3252f8795942")

    flattened = flatten_bronze_bag_pand(row)

    assert flattened["source_name"] == "bag_pdok"
    assert flattened["natural_key"] == "1"
    assert flattened["bag_id"] == "4c396a25-0e16-586f-a298-3252f8795942"
    assert flattened["pand_identificatie"] == "1960100000000001"
    assert flattened["pand_status"] == "Pand in gebruik"
    assert flattened["oorspronkelijk_bouwjaar"] == 1974
    assert flattened["geconstateerd"] == "N"
    assert flattened["documentdatum"] == "2023-03-01"
    assert flattened["documentnummer"] == "doc-123"
    assert '"type": "Polygon"' in str(flattened["geometry"])


def test_flatten_bronze_bag_pand_rows_flattens_multiple_rows_in_order() -> None:
    rows = [
        make_bag_pand_bronze_row("1", "pand-1"),
        make_bag_pand_bronze_row("2", "pand-2"),
    ]

    flattened_rows = flatten_bronze_bag_pand_rows(rows)

    assert len(flattened_rows) == 2
    assert flattened_rows[0]["bag_id"] == "pand-1"
    assert flattened_rows[1]["bag_id"] == "pand-2"


def test_bag_pand_sink_write_returns_number_of_rows() -> None:
    sink = BAGPandSink(table_ident="silver.bag_pand_flat")
    rows = [make_bag_pand_bronze_row("1", "pand-1")]
    rows = [flatten_bronze_bag_pand(row) for row in rows]
    sink._load_catalog = lambda: object()  # type: ignore[method-assign]

    class FakeTable:
        def append(self, df, snapshot_properties=None, branch=None) -> None:
            return None

    sink._replace_table = lambda catalog, arrow_schema: FakeTable()  # type: ignore[method-assign]

    written = sink.write(rows)

    assert written == 1
    assert len(sink.last_written_rows) == 1
    assert sink.last_written_rows[0]["bag_id"] == "pand-1"


def test_bag_pand_sink_write_replaces_table_and_appends_with_snapshot_properties(
    monkeypatch,
) -> None:
    sink = BAGPandSink(table_ident="silver.bag_pand_flat")
    rows = [flatten_bronze_bag_pand(make_bag_pand_bronze_row("1", "pand-1"))]

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
        "fip.source_name": "bag_pdok",
        "fip.run_id": "run-001",
        "fip.schema_version": "v1",
    }
    assert calls["branch"] is None


def test_to_bag_pand_row_returns_expected_field_order() -> None:
    flattened = flatten_bronze_bag_pand(make_bag_pand_bronze_row("1", "pand-1"))

    row = to_bag_pand_row(flattened)

    assert tuple(row.keys()) == BAG_PAND_FIELDS
    assert row["bag_id"] == "pand-1"
