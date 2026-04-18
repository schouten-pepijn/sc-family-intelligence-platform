from datetime import datetime, timezone

from fip.silver.cbs_observations import (
    SILVER_OBSERVATION_FIELDS,
    flatten_bronze_observation,
    flatten_bronze_observation_rows,
    to_silver_observation_row,
)


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


def test_flatten_bronze_observation_rows_flattens_multiple_rows_in_order() -> None:
    rows = [
        {
            "source_name": "cbs_statline",
            "natural_key": "1",
            "retrieved_at": datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
            "run_id": "run-001",
            "schema_version": "v1",
            "http_status": 200,
            "payload": (
                '{"Id": 1, "Measure": "M001534", "Perioden": "1995JJ00", '
                '"RegioS": "NL01", "StringValue": null, "Value": 93750.0, '
                '"ValueAttribute": "None"}'
            ),
        },
        {
            "source_name": "cbs_statline",
            "natural_key": "2",
            "retrieved_at": datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
            "run_id": "run-001",
            "schema_version": "v1",
            "http_status": 200,
            "payload": (
                '{"Id": 2, "Measure": "M001534", "Perioden": "1995JJ00", '
                '"RegioS": "NL01", "StringValue": null, "Value": 93750.0, '
                '"ValueAttribute": "None"}'
            ),
        },
    ]

    flattened_rows = flatten_bronze_observation_rows(rows)

    assert len(flattened_rows) == 2
    assert flattened_rows[0]["observation_id"] == 1
    assert flattened_rows[0]["natural_key"] == "1"
    assert flattened_rows[1]["observation_id"] == 2
    assert flattened_rows[1]["natural_key"] == "2"


def test_to_silver_observation_row_returns_expected_field_order() -> None:
    flattened = {
        "source_name": "cbs_statline",
        "natural_key": "42",
        "retrieved_at": datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "observation_id": 42,
        "measure_code": "M001534",
        "period_code": "1995JJ00",
        "region_code": "NL01",
        "numeric_value": 93750.0,
        "value_attribute": "None",
        "string_value": None,
    }

    row = to_silver_observation_row(flattened)

    assert tuple(row.keys()) == SILVER_OBSERVATION_FIELDS
    assert row["observation_id"] == 42
    assert row["measure_code"] == "M001534"
