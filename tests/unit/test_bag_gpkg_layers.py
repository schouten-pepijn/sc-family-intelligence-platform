import json
from datetime import datetime, timezone

from fip.gold.pdok_bag.bag_gpkg_layer_writer import BAGGpkgLayerLandingWriter
from fip.lakehouse.silver.pdok_bag.bag_gpkg_layer_sink import BAGGpkgLayerSink
from fip.lakehouse.silver.pdok_bag.bag_gpkg_layers import (
    flatten_bronze_bag_gpkg_layer,
)


def make_bronze_row(properties: dict[str, object]) -> dict[str, object]:
    return {
        "source_name": "bag_gpkg",
        "natural_key": str(properties["identificatie"]),
        "retrieved_at": datetime(2026, 4, 19, 17, 43, 42, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "payload": json.dumps(
            {
                "type": "Feature",
                "id": properties["identificatie"],
                "geometry": {"type": "Point", "coordinates": [155000, 463000]},
                "properties": properties,
            },
            ensure_ascii=False,
        ),
    }


def test_flatten_bronze_bag_gpkg_pand_maps_gpkg_contract() -> None:
    flattened = flatten_bronze_bag_gpkg_layer(
        "pand",
        make_bronze_row(
            {
                "identificatie": "pand-1",
                "bouwjaar": 1984,
                "status": "Pand in gebruik",
                "gebruiksdoel": "woonfunctie",
                "oppervlakte_min": 50,
                "oppervlakte_max": 90,
                "aantal_verblijfsobjecten": 2,
            },
        ),
    )

    assert flattened["pand_identificatie"] == "pand-1"
    assert flattened["bouwjaar"] == 1984
    assert flattened["oppervlakte_min"] == 50
    assert flattened["aantal_verblijfsobjecten"] == 2


def test_flatten_bronze_bag_gpkg_woonplaats_maps_gpkg_contract() -> None:
    flattened = flatten_bronze_bag_gpkg_layer(
        "woonplaats",
        make_bronze_row(
            {
                "identificatie": "wp-1",
                "status": "Woonplaats aangewezen",
                "woonplaats": "Amsterdam",
                "bronhouder_identificatie": "0363",
            },
        ),
    )

    assert flattened["woonplaats_identificatie"] == "wp-1"
    assert flattened["woonplaats"] == "Amsterdam"
    assert flattened["bronhouder_identificatie"] == "0363"


def test_flatten_bronze_bag_gpkg_ligplaats_maps_gpkg_contract() -> None:
    flattened = flatten_bronze_bag_gpkg_layer(
        "ligplaats",
        make_bronze_row(
            {
                "identificatie": "lig-1",
                "status": "Plaats aangewezen",
                "openbare_ruimte_naam": "Kade",
                "openbare_ruimte_naam_kort": "Kade",
                "huisnummer": 10,
                "huisletter": None,
                "toevoeging": None,
                "postcode": "1234AB",
                "woonplaats_naam": "Amsterdam",
                "nummeraanduiding_hoofdadres_identificatie": "na-1",
                "openbare_ruimte_identificatie": "or-1",
                "woonplaats_identificatie": "wp-1",
                "bronhouder_identificatie": "0363",
            },
        ),
    )

    assert flattened["ligplaats_identificatie"] == "lig-1"
    assert flattened["hoofdadres_identificatie"] == "na-1"
    assert flattened["huisnummer"] == 10


def test_bag_gpkg_layer_sink_exposes_expected_schema() -> None:
    sink = BAGGpkgLayerSink(
        layer="standplaats",
        table_ident="silver.bag_gpkg_standplaats_flat",
    )

    assert sink._get_arrow_schema().field("standplaats_identificatie").nullable is False
    assert sink._get_arrow_schema().field("huisnummer").type.bit_width == 64


def test_bag_gpkg_layer_landing_writer_uses_layer_table_columns() -> None:
    writer = BAGGpkgLayerLandingWriter(layer="pand", table_name="bag_gpkg_pand")

    columns_sql = writer._table_columns_sql()

    assert "pand_identificatie text NOT NULL" in columns_sql
    assert "bouwjaar bigint" in columns_sql
