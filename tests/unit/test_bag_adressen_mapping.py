from datetime import datetime, timezone

from fip.ingestion.pdok_bag.adressen_mapping import (
    to_bag_adressen_geo_region_mapping_row,
)


def test_to_bag_adressen_geo_region_mapping_row_uses_bronhouder_identificatie() -> None:
    bag_row = {
        "bag_id": "adres-1",
        "retrieved_at": datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
        "run_id": "run-001",
        "bronhouder_identificatie": "1883",
    }

    mapping_row = to_bag_adressen_geo_region_mapping_row(bag_row)

    assert mapping_row == {
        "bag_object_id": "adres-1",
        "bag_object_type": "bag_adres",
        "region_id": "GM1883",
        "mapping_method": "bag_ogc_v2_adres",
        "confidence": 1.0,
        "active_from": datetime(
            2026,
            4,
            19,
            17,
            43,
            42,
            77000,
            tzinfo=timezone.utc,
        ),
        "active_to": None,
        "run_id": "run-001",
    }


def test_to_bag_adressen_geo_region_mapping_row_accepts_gemeente_code_alias() -> None:
    bag_row = {
        "bag_id": "adres-1",
        "retrieved_at": datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
        "run_id": "run-001",
        "gemeente_code": "1883",
    }

    mapping_row = to_bag_adressen_geo_region_mapping_row(bag_row)

    assert mapping_row["region_id"] == "GM1883"
