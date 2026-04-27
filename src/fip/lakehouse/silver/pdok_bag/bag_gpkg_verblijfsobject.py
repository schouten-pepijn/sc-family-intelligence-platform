import json
from math import isnan

BAG_GPKG_VERBLIJFSOBJECT_FIELDS = (
    "source_name",
    "natural_key",
    "retrieved_at",
    "run_id",
    "schema_version",
    "http_status",
    "bag_id",
    "verblijfsobject_identificatie",
    "hoofdadres_identificatie",
    "postcode",
    "huisnummer",
    "huisletter",
    "toevoeging",
    "woonplaats_naam",
    "openbare_ruimte_naam",
    "gebruiksdoel",
    "oppervlakte",
    "geometry",
)


def to_bag_gpkg_verblijfsobject_row(row: dict[str, object]) -> dict[str, object]:
    return {field: row[field] for field in BAG_GPKG_VERBLIJFSOBJECT_FIELDS}


def flatten_bronze_bag_gpkg_verblijfsobject(row: dict[str, object]) -> dict[str, object]:
    payload_raw = row["payload"]
    if not isinstance(payload_raw, str):
        raise ValueError("Expected 'payload' field to be a string")

    payload = json.loads(payload_raw)
    properties = payload["properties"]
    if not isinstance(properties, dict):
        raise ValueError("Expected BAG GPKG 'properties' field to be an object")

    geometry = payload.get("geometry")
    geometry_json = (
        json.dumps(geometry, sort_keys=True, ensure_ascii=False) if geometry is not None else None
    )

    return {
        "source_name": row["source_name"],
        "natural_key": row["natural_key"],
        "retrieved_at": row["retrieved_at"],
        "run_id": row["run_id"],
        "schema_version": row["schema_version"],
        "http_status": row["http_status"],
        "bag_id": payload["id"],
        "verblijfsobject_identificatie": _clean_value(properties["identificatie"]),
        "hoofdadres_identificatie": _clean_value(
            properties.get("nummeraanduiding_hoofdadres_identificatie")
        ),
        "postcode": _clean_value(properties.get("postcode")),
        "huisnummer": _clean_value(properties.get("huisnummer")),
        "huisletter": _clean_value(properties.get("huisletter")),
        "toevoeging": _clean_value(properties.get("toevoeging")),
        "woonplaats_naam": _clean_value(properties.get("woonplaats_naam")),
        "openbare_ruimte_naam": _clean_value(properties.get("openbare_ruimte_naam")),
        "gebruiksdoel": _clean_value(properties.get("gebruiksdoel")),
        "oppervlakte": _clean_value(properties.get("oppervlakte")),
        "geometry": geometry_json,
    }


def flatten_bronze_bag_gpkg_verblijfsobject_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [flatten_bronze_bag_gpkg_verblijfsobject(row) for row in rows]


def _clean_value(value: object) -> object | None:
    if value is None:
        return None
    if value.__class__.__name__ in {"NAType", "NaTType"}:
        return None
    if isinstance(value, float) and isnan(value):
        return None
    try:
        return None if value != value else value
    except (TypeError, ValueError):
        return value
