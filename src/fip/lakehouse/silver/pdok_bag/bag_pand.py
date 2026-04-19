import json

BAG_PAND_FIELDS = (
    "source_name",
    "natural_key",
    "retrieved_at",
    "run_id",
    "schema_version",
    "http_status",
    "bag_id",
    "pand_identificatie",
    "pand_status",
    "oorspronkelijk_bouwjaar",
    "geconstateerd",
    "documentdatum",
    "documentnummer",
    "geometry",
)


def to_bag_pand_row(row: dict[str, object]) -> dict[str, object]:
    return {field: row[field] for field in BAG_PAND_FIELDS}


def flatten_bronze_bag_pand(row: dict[str, object]) -> dict[str, object]:
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
    oorspronkelijk_bouwjaar = properties.get("oorspronkelijk_bouwjaar")

    return {
        "source_name": row["source_name"],
        "natural_key": row["natural_key"],
        "retrieved_at": row["retrieved_at"],
        "run_id": row["run_id"],
        "schema_version": row["schema_version"],
        "http_status": row["http_status"],
        "bag_id": payload["id"],
        "pand_identificatie": properties["identificatie"],
        "pand_status": properties.get("status"),
        "oorspronkelijk_bouwjaar": (
            int(oorspronkelijk_bouwjaar) if oorspronkelijk_bouwjaar is not None else None
        ),
        "geconstateerd": properties.get("geconstateerd"),
        "documentdatum": properties.get("documentdatum"),
        "documentnummer": properties.get("documentnummer"),
        "geometry": geometry_json,
    }


def flatten_bronze_bag_pand_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [flatten_bronze_bag_pand(row) for row in rows]
