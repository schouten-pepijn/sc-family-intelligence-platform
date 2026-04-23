import json

BAG_ADRESSEN_FIELDS = (
    "source_name",
    "natural_key",
    "retrieved_at",
    "run_id",
    "schema_version",
    "http_status",
    "bag_id",
    "adres_identificatie",
    "postcode",
    "huisnummer",
    "huisletter",
    "toevoeging",
    "openbare_ruimte_naam",
    "woonplaats_naam",
    "bronhouder_identificatie",
    "gemeentecode",
    "gemeentenaam",
    "geometry",
)


def to_bag_adressen_row(row: dict[str, object]) -> dict[str, object]:
    return {field: row[field] for field in BAG_ADRESSEN_FIELDS}


def flatten_bronze_bag_adressen(row: dict[str, object]) -> dict[str, object]:
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
        "adres_identificatie": properties.get("identificatie"),
        "postcode": properties.get("postcode"),
        "huisnummer": _coerce_optional_int(properties.get("huisnummer"), "huisnummer"),
        "huisletter": properties.get("huisletter"),
        "toevoeging": properties.get("toevoeging"),
        "openbare_ruimte_naam": properties.get("openbare_ruimte_naam"),
        "woonplaats_naam": properties.get("woonplaats_naam"),
        "bronhouder_identificatie": _coerce_optional_text(
            properties.get("bronhouder_identificatie"),
            "bronhouder_identificatie",
        ),
        "gemeentecode": _derive_gemeentecode(properties),
        "gemeentenaam": properties.get("bronhouder_naam")
        or properties.get("gemeentenaam")
        or properties.get("gemeente_naam"),
        "geometry": geometry_json,
    }


def _coerce_optional_int(value: object, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"Expected '{field}' to be an integer, not a boolean")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError as exc:
            raise ValueError(f"Expected '{field}' to be an integer-like string") from exc

    raise ValueError(f"Expected '{field}' to be an integer or integer-like string")


def _coerce_optional_text(value: object, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Expected '{field}' to be a string or null")

    text = value.strip()
    return text or None


def _derive_gemeentecode(properties: dict[str, object]) -> str | None:
    bronhouder_identificatie = _coerce_optional_text(
        properties.get("bronhouder_identificatie"),
        "bronhouder_identificatie",
    )
    if bronhouder_identificatie:
        return f"GM{bronhouder_identificatie.zfill(4)}"

    gemeentecode = _coerce_optional_text(
        properties.get("gemeentecode") or properties.get("gemeente_code"),
        "gemeentecode",
    )
    if gemeentecode:
        normalized = gemeentecode.upper()
        if normalized.startswith("GM"):
            return normalized
        if normalized.isdigit():
            return f"GM{normalized.zfill(4)}"
        return normalized

    return None


def flatten_bronze_bag_adressen_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [flatten_bronze_bag_adressen(row) for row in rows]
