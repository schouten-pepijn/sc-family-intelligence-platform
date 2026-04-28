import json
from dataclasses import dataclass
from math import isnan
from typing import SupportsInt, cast


@dataclass(frozen=True)
class BAGGpkgLayerConfig:
    layer: str
    fields: tuple[str, ...]
    property_to_column: dict[str, str]
    int_columns: frozenset[str]


BASE_FIELDS = (
    "source_name",
    "natural_key",
    "retrieved_at",
    "run_id",
    "schema_version",
    "http_status",
    "bag_id",
)

BAG_GPKG_LAYER_CONFIGS: dict[str, BAGGpkgLayerConfig] = {
    "pand": BAGGpkgLayerConfig(
        layer="pand",
        fields=(
            *BASE_FIELDS,
            "pand_identificatie",
            "bouwjaar",
            "status",
            "gebruiksdoel",
            "oppervlakte_min",
            "oppervlakte_max",
            "aantal_verblijfsobjecten",
            "geometry",
        ),
        property_to_column={
            "identificatie": "pand_identificatie",
            "bouwjaar": "bouwjaar",
            "status": "status",
            "gebruiksdoel": "gebruiksdoel",
            "oppervlakte_min": "oppervlakte_min",
            "oppervlakte_max": "oppervlakte_max",
            "aantal_verblijfsobjecten": "aantal_verblijfsobjecten",
        },
        int_columns=frozenset(
            {
                "bouwjaar",
                "oppervlakte_min",
                "oppervlakte_max",
                "aantal_verblijfsobjecten",
            }
        ),
    ),
    "woonplaats": BAGGpkgLayerConfig(
        layer="woonplaats",
        fields=(
            *BASE_FIELDS,
            "woonplaats_identificatie",
            "status",
            "woonplaats",
            "bronhouder_identificatie",
            "geometry",
        ),
        property_to_column={
            "identificatie": "woonplaats_identificatie",
            "status": "status",
            "woonplaats": "woonplaats",
            "bronhouder_identificatie": "bronhouder_identificatie",
        },
        int_columns=frozenset(),
    ),
    "ligplaats": BAGGpkgLayerConfig(
        layer="ligplaats",
        fields=(
            *BASE_FIELDS,
            "ligplaats_identificatie",
            "status",
            "openbare_ruimte_naam",
            "openbare_ruimte_naam_kort",
            "huisnummer",
            "huisletter",
            "toevoeging",
            "postcode",
            "woonplaats_naam",
            "hoofdadres_identificatie",
            "openbare_ruimte_identificatie",
            "woonplaats_identificatie",
            "bronhouder_identificatie",
            "geometry",
        ),
        property_to_column={
            "identificatie": "ligplaats_identificatie",
            "status": "status",
            "openbare_ruimte_naam": "openbare_ruimte_naam",
            "openbare_ruimte_naam_kort": "openbare_ruimte_naam_kort",
            "huisnummer": "huisnummer",
            "huisletter": "huisletter",
            "toevoeging": "toevoeging",
            "postcode": "postcode",
            "woonplaats_naam": "woonplaats_naam",
            "nummeraanduiding_hoofdadres_identificatie": "hoofdadres_identificatie",
            "openbare_ruimte_identificatie": "openbare_ruimte_identificatie",
            "woonplaats_identificatie": "woonplaats_identificatie",
            "bronhouder_identificatie": "bronhouder_identificatie",
        },
        int_columns=frozenset({"huisnummer"}),
    ),
    "standplaats": BAGGpkgLayerConfig(
        layer="standplaats",
        fields=(
            *BASE_FIELDS,
            "standplaats_identificatie",
            "status",
            "openbare_ruimte_naam",
            "openbare_ruimte_naam_kort",
            "huisnummer",
            "huisletter",
            "toevoeging",
            "postcode",
            "woonplaats_naam",
            "hoofdadres_identificatie",
            "openbare_ruimte_identificatie",
            "woonplaats_identificatie",
            "bronhouder_identificatie",
            "geometry",
        ),
        property_to_column={
            "identificatie": "standplaats_identificatie",
            "status": "status",
            "openbare_ruimte_naam": "openbare_ruimte_naam",
            "openbare_ruimte_naam_kort": "openbare_ruimte_naam_kort",
            "huisnummer": "huisnummer",
            "huisletter": "huisletter",
            "toevoeging": "toevoeging",
            "postcode": "postcode",
            "woonplaats_naam": "woonplaats_naam",
            "nummeraanduiding_hoofdadres_identificatie": "hoofdadres_identificatie",
            "openbare_ruimte_identificatie": "openbare_ruimte_identificatie",
            "woonplaats_identificatie": "woonplaats_identificatie",
            "bronhouder_identificatie": "bronhouder_identificatie",
        },
        int_columns=frozenset({"huisnummer"}),
    ),
}


def to_bag_gpkg_layer_row(layer: str, row: dict[str, object]) -> dict[str, object]:
    config = _config(layer)
    return {field: row[field] for field in config.fields}


def flatten_bronze_bag_gpkg_layer(layer: str, row: dict[str, object]) -> dict[str, object]:
    config = _config(layer)
    payload_raw = row["payload"]
    if not isinstance(payload_raw, str):
        raise ValueError("Expected 'payload' field to be a string")

    payload = json.loads(payload_raw)
    properties = payload["properties"]
    if not isinstance(properties, dict):
        raise ValueError("Expected BAG GPKG 'properties' field to be an object")

    geometry = payload.get("geometry")
    flattened: dict[str, object] = {
        "source_name": row["source_name"],
        "natural_key": row["natural_key"],
        "retrieved_at": row["retrieved_at"],
        "run_id": row["run_id"],
        "schema_version": row["schema_version"],
        "http_status": row["http_status"],
        "bag_id": payload["id"],
        "geometry": (
            json.dumps(geometry, sort_keys=True, ensure_ascii=False)
            if geometry is not None
            else None
        ),
    }

    for property_name, column_name in config.property_to_column.items():
        value = _clean_value(properties.get(property_name))
        flattened[column_name] = _as_int(value) if column_name in config.int_columns else value

    return flattened


def flatten_bronze_bag_gpkg_layer_rows(
    layer: str,
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [flatten_bronze_bag_gpkg_layer(layer, row) for row in rows]


def _config(layer: str) -> BAGGpkgLayerConfig:
    config = BAG_GPKG_LAYER_CONFIGS.get(layer)
    if config is None:
        supported = ", ".join(sorted(BAG_GPKG_LAYER_CONFIGS))
        raise ValueError(
            f"Unsupported BAG GPKG Silver layer '{layer}'. Expected one of: {supported}"
        )
    return config


def _as_int(value: object) -> int | None:
    if value is None:
        return None
    return int(cast(SupportsInt | str | bytes | bytearray, value))


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
