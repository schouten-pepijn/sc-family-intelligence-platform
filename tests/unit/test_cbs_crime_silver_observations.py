from __future__ import annotations

from datetime import datetime, timezone

import pytest

from fip.lakehouse.silver.cbs_crime.cbs_crime_observations import (
    flatten_bronze_crime_observation,
)


def make_bronze_row(payload: str, entity_name: str = "83648NED.Observations") -> dict[str, object]:
    return {
        "source_name": "cbs_crime",
        "entity_name": entity_name,
        "natural_key": "1",
        "retrieved_at": datetime(2026, 5, 3, tzinfo=timezone.utc),
        "run_id": "crime-run",
        "schema_version": "v1",
        "http_status": 200,
        "payload": payload,
    }


def test_flatten_bronze_crime_observation_flattens_payload() -> None:
    row = make_bronze_row(
        (
            '{"Id": 1, "Measure": "M004200_2", "SoortMisdrijf": "1.1", '
            '"RegioS": "GM0363", "Perioden": "2024JJ00", "Value": "123", '
            '"ValueAttribute": null}'
        )
    )

    silver_row = flatten_bronze_crime_observation(row)

    assert silver_row["source_name"] == "cbs_crime"
    assert silver_row["entity_name"] == "83648NED.Observations"
    assert silver_row["observation_id"] == "1"
    assert silver_row["measure_id"] == "M004200_2"
    assert silver_row["crime_type_id"] == "1.1"
    assert silver_row["region_id"] == "GM0363"
    assert silver_row["period_id"] == "2024JJ00"
    assert silver_row["period_year"] == 2024
    assert silver_row["value"] == 123.0
    assert silver_row["value_attribute"] is None


def test_flatten_bronze_crime_observation_preserves_missing_value_attribute() -> None:
    row = make_bronze_row(
        (
            '{"Id": 1, "Measure": "M004200_2", "SoortMisdrijf": "1.1", '
            '"RegioS": "GM0363", "Perioden": "2024JJ00", "Value": null, '
            '"ValueAttribute": "Impossible"}'
        )
    )

    silver_row = flatten_bronze_crime_observation(row)

    assert silver_row["value"] is None
    assert silver_row["value_attribute"] == "Impossible"


def test_flatten_bronze_crime_observation_raises_for_non_observation_entity() -> None:
    row = make_bronze_row('{"Id": "1.1"}', entity_name="83648NED.SoortMisdrijfCodes")

    with pytest.raises(ValueError, match="Expected CBS crime Observations row"):
        flatten_bronze_crime_observation(row)


def test_flatten_bronze_crime_observation_raises_for_missing_required_payload_field() -> None:
    row = make_bronze_row(
        (
            '{"Id": 1, "Measure": "M004200_2", "RegioS": "GM0363", '
            '"Perioden": "2024JJ00", "Value": "123", "ValueAttribute": null}'
        )
    )

    with pytest.raises(
        ValueError, match="Missing required CBS crime payload field 'SoortMisdrijf'"
    ):
        flatten_bronze_crime_observation(row)


def test_flatten_bronze_crime_observation_raises_for_invalid_numeric_value() -> None:
    row = make_bronze_row(
        (
            '{"Id": 1, "Measure": "M004200_2", "SoortMisdrijf": "1.1", '
            '"RegioS": "GM0363", "Perioden": "2024JJ00", "Value": "not-a-number", '
            '"ValueAttribute": null}'
        )
    )

    with pytest.raises(ValueError, match="Invalid CBS crime numeric value"):
        flatten_bronze_crime_observation(row)
