import json
from datetime import datetime, timezone
from typing import cast

from pyiceberg.table import Table

from fip.lakehouse.silver.pdok_bag.bag_gpkg_verblijfsobject import (
    BAG_GPKG_VERBLIJFSOBJECT_FIELDS,
    flatten_bronze_bag_gpkg_verblijfsobject,
    flatten_bronze_bag_gpkg_verblijfsobject_rows,
    to_bag_gpkg_verblijfsobject_row,
)
from fip.lakehouse.silver.pdok_bag.bag_gpkg_verblijfsobject_sink import (
    BAGGpkgVerblijfsobjectSink,
)


def make_bag_gpkg_bronze_row(natural_key: str, bag_id: str) -> dict[str, object]:
    return {
        "source_name": "bag_gpkg",
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
                    "type": "Point",
                    "coordinates": [5.862878870591695, 50.99994879976323],
                },
                "properties": {
                    "identificatie": "0000010000057469",
                    "hoofdadres_identificatie": "0000200000057534",
                    "postcode": "6131BE",
                    "huisnummer": 32,
                    "huisletter": "A",
                    "toevoeging": None,
                    "woonplaats_naam": "Sittard",
                    "openbare_ruimte_naam": "Steenweg",
                    "gebruiksdoel": "woonfunctie",
                    "oppervlakte": 72,
                },
            },
            ensure_ascii=False,
        ),
    }


def test_flatten_bronze_bag_gpkg_verblijfsobject_maps_payload_fields_to_silver_columns() -> None:
    row = make_bag_gpkg_bronze_row("1", "80f96ef7-dfa4-5197-b681-cfd92b10757e")

    flattened = flatten_bronze_bag_gpkg_verblijfsobject(row)

    assert flattened["source_name"] == "bag_gpkg"
    assert flattened["natural_key"] == "1"
    assert flattened["bag_id"] == "80f96ef7-dfa4-5197-b681-cfd92b10757e"
    assert flattened["verblijfsobject_identificatie"] == "0000010000057469"
    assert flattened["hoofdadres_identificatie"] == "0000200000057534"
    assert flattened["postcode"] == "6131BE"
    assert flattened["huisnummer"] == 32
    assert flattened["huisletter"] == "A"
    assert flattened["toevoeging"] is None
    assert flattened["woonplaats_naam"] == "Sittard"
    assert flattened["openbare_ruimte_naam"] == "Steenweg"
    assert flattened["gebruiksdoel"] == "woonfunctie"
    assert flattened["oppervlakte"] == 72
    assert flattened["geometry"] == (
        '{"coordinates": [5.862878870591695, 50.99994879976323], "type": "Point"}'
    )


def test_flatten_bronze_bag_gpkg_verblijfsobject_rows_flattens_multiple_rows_in_order() -> None:
    rows = [
        make_bag_gpkg_bronze_row("1", "bag-1"),
        make_bag_gpkg_bronze_row("2", "bag-2"),
    ]

    flattened_rows = flatten_bronze_bag_gpkg_verblijfsobject_rows(rows)

    assert len(flattened_rows) == 2
    assert flattened_rows[0]["bag_id"] == "bag-1"
    assert flattened_rows[1]["bag_id"] == "bag-2"


def test_bag_gpkg_verblijfsobject_sink_write_returns_number_of_rows() -> None:
    sink = BAGGpkgVerblijfsobjectSink(table_ident="silver.bag_gpkg_verblijfsobject_flat")
    rows = [flatten_bronze_bag_gpkg_verblijfsobject(make_bag_gpkg_bronze_row("1", "bag-1"))]
    sink._load_catalog = lambda: object()  # type: ignore[method-assign]

    class FakeTable:
        def append(self, df, snapshot_properties=None, branch=None) -> None:
            return None

    def fake_replace_table(catalog: object, arrow_schema: object) -> Table:
        return cast(Table, FakeTable())

    sink._replace_table = fake_replace_table  # type: ignore[method-assign]

    written = sink.write(rows)

    assert written == 1
    assert len(sink.last_written_rows) == 1
    assert sink.last_written_rows[0]["bag_id"] == "bag-1"


def test_to_bag_gpkg_verblijfsobject_row_returns_expected_field_order() -> None:
    flattened = flatten_bronze_bag_gpkg_verblijfsobject(make_bag_gpkg_bronze_row("1", "bag-1"))

    row = to_bag_gpkg_verblijfsobject_row(flattened)

    assert tuple(row.keys()) == BAG_GPKG_VERBLIJFSOBJECT_FIELDS
    assert row["bag_id"] == "bag-1"
