from __future__ import annotations

import json
from typing import Any

SILVER_CRIME_OBSERVATION_FIELDS = (
    "source_name",
    "entity_name",
    "natural_key",
    "retrieved_at",
    "run_id",
    "schema_version",
    "http_status",
    "observation_id",
    "measure_id",
    "crime_type_id",
    "region_id",
    "period_id",
    "period_year",
    "value",
    "value_attribute",
)


def to_silver_crime_observation_row(row: dict[str, object]) -> dict[str, object]:
    return {field: row[field] for field in SILVER_CRIME_OBSERVATION_FIELDS}


def flatten_bronze_crime_observation(row: dict[str, object]) -> dict[str, object]:
    """Flatten one CBS crime Bronze observation into the Silver crime fact shape."""
    entity_name = _require_row_text(row, "entity_name")
    if not entity_name.endswith(".Observations"):
        raise ValueError(f"Expected CBS crime Observations row, got '{entity_name}'")

    payload = _parse_payload(row)
    period_id = _require_payload_text(payload, "Perioden")

    return {
        "source_name": _require_row_text(row, "source_name"),
        "entity_name": entity_name,
        "natural_key": _require_row_text(row, "natural_key"),
        "retrieved_at": row["retrieved_at"],
        "run_id": _require_row_text(row, "run_id"),
        "schema_version": _require_row_text(row, "schema_version"),
        "http_status": _require_row_int(row, "http_status"),
        "observation_id": _require_payload_text(payload, "Id"),
        "measure_id": _require_payload_text(payload, "Measure"),
        "crime_type_id": _require_payload_text(payload, "SoortMisdrijf"),
        "region_id": _require_payload_text(payload, "RegioS"),
        "period_id": period_id,
        "period_year": _period_year(period_id),
        "value": _optional_float(payload.get("Value")),
        "value_attribute": _optional_text(payload.get("ValueAttribute")),
    }


def flatten_bronze_crime_observation_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [
        flatten_bronze_crime_observation(row)
        for row in rows
        if _require_row_text(row, "entity_name").endswith(".Observations")
    ]


def _parse_payload(row: dict[str, object]) -> dict[str, Any]:
    payload_raw = row.get("payload")
    if not isinstance(payload_raw, str):
        raise ValueError("Expected 'payload' field to be a string")

    payload = json.loads(payload_raw)
    if not isinstance(payload, dict):
        raise ValueError("Expected CBS crime payload to decode to an object")
    return payload


def _require_row_text(row: dict[str, object], field: str) -> str:
    value = row.get(field)
    if value in (None, ""):
        raise ValueError(f"Missing required CBS crime Bronze field '{field}'")
    return str(value)


def _require_row_int(row: dict[str, object], field: str) -> int:
    value = row.get(field)
    if value in (None, ""):
        raise ValueError(f"Missing required CBS crime Bronze field '{field}'")
    if not isinstance(value, int | str):
        raise ValueError(f"Invalid CBS crime Bronze integer field '{field}': {value!r}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid CBS crime Bronze integer field '{field}': {value!r}") from exc


def _require_payload_text(payload: dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if value in (None, ""):
        raise ValueError(f"Missing required CBS crime payload field '{field}'")
    return str(value)


def _optional_text(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    if not isinstance(value, int | float | str):
        raise ValueError(f"Invalid CBS crime numeric value: {value!r}")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid CBS crime numeric value: {value!r}") from exc


def _period_year(period_id: str) -> int | None:
    digits = "".join(char for char in period_id if char.isdigit())
    if len(digits) < 4:
        return None
    return int(digits[:4])
