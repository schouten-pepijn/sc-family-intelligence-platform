from collections.abc import Mapping
from datetime import datetime


def to_bag_adressen_geo_region_mapping_row(
    bag_row: Mapping[str, object],
    *,
    bag_object_type: str = "bag_adres",
    mapping_method: str = "bag_ogc_v2_adres",
) -> dict[str, object]:
    bag_object_id = _require_text(bag_row, "bag_id")
    region_id = _require_gemeentecode(bag_row)
    active_from = _require_datetime(bag_row, "retrieved_at")

    return {
        "bag_object_id": bag_object_id,
        "bag_object_type": bag_object_type,
        "region_id": region_id,
        "mapping_method": mapping_method,
        "confidence": 1.0,
        "active_from": active_from,
        "active_to": None,
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


def _require_text_from_any(
    row: Mapping[str, object],
    fields: tuple[str, ...],
) -> str:
    for field in fields:
        value = row.get(field)
        if isinstance(value, str) and value:
            return value

    joined_fields = ", ".join(fields)
    raise ValueError(f"Expected one of {joined_fields} to be a non-empty string")


def _require_gemeentecode(row: Mapping[str, object]) -> str:
    code = _require_text_from_any(
        row,
        (
            "bronhouder_identificatie",
            "gemeentecode",
            "gemeente_code",
            "municipality_code",
            "region_id",
        ),
    )
    normalized = code.strip().upper()
    if normalized.startswith("GM") and normalized[2:].isdigit():
        return normalized
    if normalized.isdigit():
        return f"GM{normalized.zfill(4)}"

    raise ValueError(
        "Expected 'bronhouder_identificatie' or a municipal code that can be normalized to GMxxxx"
    )
