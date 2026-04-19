import json

BAG_VERBLIJFSOBJECT_FIELDS = (
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


def to_bag_verblijfsobject_row(row: dict[str, object]) -> dict[str, object]:
    return {field: row[field] for field in BAG_VERBLIJFSOBJECT_FIELDS}


def flatten_bronze_bag_verblijfsobject(row: dict[str, object]) -> dict[str, object]:
    payload_raw = row["payload"]
    if not isinstance(payload_raw, str):
        raise ValueError("Expected 'payload' field to be a string")

    payload = json.loads(payload_raw)
    properties = payload["properties"]
    if not isinstance(properties, dict):
        raise ValueError("Expected BAG 'properties' field to be an object")

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
        "verblijfsobject_identificatie": properties["identificatie"],
        "hoofdadres_identificatie": properties["hoofdadres_identificatie"],
        "postcode": properties["postcode"],
        "huisnummer": properties["huisnummer"],
        "huisletter": properties["huisletter"],
        "toevoeging": properties["toevoeging"],
        "woonplaats_naam": properties["woonplaats_naam"],
        "openbare_ruimte_naam": properties["openbare_ruimte_naam"],
        "gebruiksdoel": properties["gebruiksdoel"],
        "oppervlakte": properties["oppervlakte"],
        "geometry": geometry_json,
    }


def flatten_bronze_bag_verblijfsobject_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [flatten_bronze_bag_verblijfsobject(row) for row in rows]
