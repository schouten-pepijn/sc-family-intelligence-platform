from datetime import datetime, timezone

from fip.ingestion.locatieserver.mapping import to_bag_geo_region_mapping_row


def test_to_bag_geo_region_mapping_row_uses_bag_row_and_lookup_result() -> None:
    bag_row = {
        "bag_id": "80f96ef7-dfa4-5197-b681-cfd92b10757e",
        "retrieved_at": datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
    }
    lookup_result = {
        "response": {
            "docs": [
                {
                    "id": "GM1883",
                    "score": 0.92,
                }
            ]
        }
    }

    mapping_row = to_bag_geo_region_mapping_row(bag_row, lookup_result)

    assert mapping_row == {
        "bag_object_id": "80f96ef7-dfa4-5197-b681-cfd92b10757e",
        "bag_object_type": "bag_verblijfsobject",
        "region_id": "GM1883",
        "mapping_method": "locatieserver_postcode",
        "confidence": 0.92,
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
    }


def test_to_bag_geo_region_mapping_row_raises_when_region_is_missing() -> None:
    bag_row = {
        "bag_id": "80f96ef7-dfa4-5197-b681-cfd92b10757e",
        "retrieved_at": datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
    }

    try:
        to_bag_geo_region_mapping_row(bag_row, {"response": {"docs": [{}]}})
    except ValueError as exc:
        assert "region identifier" in str(exc)
    else:
        raise AssertionError("Expected ValueError when region_id is missing")
