from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime


def to_bag_geo_region_mapping_row(
    bag_row: Mapping[str, object],
    lookup_result: Mapping[str, object],
    *,
    bag_object_type: str = "bag_verblijfsobject",
    mapping_method: str = "locatieserver_postcode",
) -> dict[str, object]:
    bag_object_id = _require_text(bag_row, "bag_id")
    region_id = _extract_region_id(lookup_result)
    confidence = _extract_confidence(lookup_result)
    active_from = _require_datetime(bag_row, "retrieved_at")
    run_id = _require_text(bag_row, "run_id")

    return {
        "bag_object_id": bag_object_id,
        "bag_object_type": bag_object_type,
        "region_id": region_id,
        "mapping_method": mapping_method,
        "confidence": confidence,
        "active_from": active_from,
        "active_to": None,
        "run_id": run_id,
    }


def _require_text(row: Mapping[str, object], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Expected '{field}' to be a non-empty string")

    return value


def _require_datetime(row: Mapping[str, object], field: str) -> datetime:
    value = row.get(field)
    if not isinstance(value, datetime):
        raise ValueError(f"Expected '{field}' to be a datetime")

    return value


def _extract_region_id(lookup_result: Mapping[str, object]) -> str:
    value = _find_first_text_value(
        lookup_result,
        (
            "region_id",
            "regionId",
            "region",
            "code",
            "identificatie",
            "identifier",
            "id",
        ),
    )
    if value is None:
        raise ValueError("Locatieserver response did not contain a region identifier")
    return value


def _extract_confidence(lookup_result: Mapping[str, object]) -> float:
    value = _find_first_numeric_value(lookup_result, ("score", "confidence"))
    if value is None:
        return 1.0
    return float(value)


def _find_first_text_value(value: object, keys: tuple[str, ...]) -> str | None:
    if isinstance(value, Mapping):
        for key in keys:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate:
                return candidate

        for nested_key in ("response", "docs", "features", "properties", "result"):
            nested_value = value.get(nested_key)
            found = _find_first_text_value(nested_value, keys)
            if found is not None:
                return found

    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for item in value:
            found = _find_first_text_value(item, keys)
            if found is not None:
                return found

    return None


def _find_first_numeric_value(value: object, keys: tuple[str, ...]) -> float | int | None:
    if isinstance(value, Mapping):
        for key in keys:
            candidate = value.get(key)
            if isinstance(candidate, (int, float)):
                return candidate

        for nested_key in ("response", "docs", "features", "properties", "result"):
            nested_value = value.get(nested_key)
            found = _find_first_numeric_value(nested_value, keys)
            if found is not None:
                return found

    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for item in value:
            found = _find_first_numeric_value(item, keys)
            if found is not None:
                return found

    return None
