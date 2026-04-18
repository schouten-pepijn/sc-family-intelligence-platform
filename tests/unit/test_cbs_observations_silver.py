from datetime import datetime, timezone

from fip.silver.cbs_observations import flatten_bronze_observation


def test_flatten_bronze_observation_maps_payload_fields_to_silver_columns() -> None:
    row = {
        "source_name": "cbs_statline",
        "natural_key": "42",
        "retrieved_at": datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "payload": (
            '{"Id": 42, "Measure": "M001534", "Perioden": "1995JJ00", '
            '"RegioS": "NL01", "StringValue": null, "Value": 93750.0, '
            '"ValueAttribute": "None"}'
        ),
    }

    flattened = flatten_bronze_observation(row)

    assert flattened["source_name"] == "cbs_statline"
    assert flattened["natural_key"] == "42"
    assert flattened["run_id"] == "run-001"
    assert flattened["observation_id"] == 42
    assert flattened["measure_code"] == "M001534"
    assert flattened["period_code"] == "1995JJ00"
    assert flattened["region_code"] == "NL01"
    assert flattened["numeric_value"] == 93750.0
    assert flattened["value_attribute"] == "None"
    assert flattened["string_value"] is None
