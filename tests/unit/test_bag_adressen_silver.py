import json
from datetime import datetime, timezone
from typing import cast

from pyiceberg.table import Table

from fip.lakehouse.silver.pdok_bag.bag_adressen import (
    BAG_ADRESSEN_FIELDS,
    flatten_bronze_bag_adressen,
    flatten_bronze_bag_adressen_rows,
    to_bag_adressen_row,
)
from fip.lakehouse.silver.pdok_bag.bag_adressen_sink import BAGAdressenSink


def make_bag_adressen_bronze_row(natural_key: str, bag_id: str) -> dict[str, object]:
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
                    "type": "Point",
                    "coordinates": [5.862878870591695, 50.99994879976323],
                },
                "properties": {
                    "identificatie": "0003010000126809",
                    "postcode": "9901CP",
                    "huisnummer": 32,
                    "huisletter": "A",
                    "toevoeging": None,
                    "openbare_ruimte_naam": "Steenweg",
                    "woonplaats_naam": "Sittard",
                    "bronhouder_identificatie": "1883",
                    "bronhouder_naam": "Sittard-Geleen",
                },
            },
            ensure_ascii=False,
        ),
    }


def test_flatten_bronze_bag_adressen_maps_payload_fields_to_silver_columns() -> None:
    row = make_bag_adressen_bronze_row("1", "adres-1")

    flattened = flatten_bronze_bag_adressen(row)

    assert flattened["source_name"] == "bag_pdok"
    assert flattened["natural_key"] == "1"
    assert flattened["bag_id"] == "adres-1"
    assert flattened["adres_identificatie"] == "0003010000126809"
    assert flattened["postcode"] == "9901CP"
    assert flattened["huisnummer"] == 32
    assert flattened["huisletter"] == "A"
    assert flattened["toevoeging"] is None
    assert flattened["openbare_ruimte_naam"] == "Steenweg"
    assert flattened["woonplaats_naam"] == "Sittard"
    assert flattened["bronhouder_identificatie"] == "1883"
    assert flattened["gemeentecode"] == "GM1883"
    assert flattened["gemeentenaam"] == "Sittard-Geleen"
    assert flattened["geometry"] == (
        '{"coordinates": [5.862878870591695, 50.99994879976323], "type": "Point"}'
    )


def test_flatten_bronze_bag_adressen_coerces_huisnummer_string_to_int() -> None:
    row = make_bag_adressen_bronze_row("1", "adres-1")
    payload = json.loads(row["payload"])
    payload["properties"]["huisnummer"] = "32"
    row["payload"] = json.dumps(payload, ensure_ascii=False)

    flattened = flatten_bronze_bag_adressen(row)

    assert flattened["huisnummer"] == 32


def test_flatten_bronze_bag_adressen_rows_flattens_multiple_rows_in_order() -> None:
    rows = [
        make_bag_adressen_bronze_row("1", "adres-1"),
        make_bag_adressen_bronze_row("2", "adres-2"),
    ]

    flattened_rows = flatten_bronze_bag_adressen_rows(rows)

    assert len(flattened_rows) == 2
    assert flattened_rows[0]["bag_id"] == "adres-1"
    assert flattened_rows[1]["bag_id"] == "adres-2"


def test_bag_adressen_sink_write_returns_number_of_rows() -> None:
    sink = BAGAdressenSink(table_ident="silver.bag_adressen_flat")
    rows = [flatten_bronze_bag_adressen(make_bag_adressen_bronze_row("1", "adres-1"))]
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
    assert sink.last_written_rows[0]["bag_id"] == "adres-1"


def test_to_bag_adressen_row_returns_expected_field_order() -> None:
    flattened = flatten_bronze_bag_adressen(make_bag_adressen_bronze_row("1", "adres-1"))

    row = to_bag_adressen_row(flattened)

    assert tuple(row.keys()) == BAG_ADRESSEN_FIELDS
    assert row["bag_id"] == "adres-1"
