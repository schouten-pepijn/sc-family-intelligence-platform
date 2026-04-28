from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from math import isnan
from pathlib import Path
from typing import Any, SupportsInt

import pyogrio
from shapely.geometry import mapping

from fip.ingestion.base import RawRecord

GPKG_URL = "https://service.pdok.nl/kadaster/bag/atom/downloads/bag-light.gpkg"

GPKG_LAYER_FIELDS: dict[str, tuple[str, ...]] = {
    "verblijfsobject": (
        "identificatie",
        "oppervlakte",
        "status",
        "gebruiksdoel",
        "openbare_ruimte_naam",
        "openbare_ruimte_naam_kort",
        "huisnummer",
        "huisletter",
        "toevoeging",
        "postcode",
        "woonplaats_naam",
        "bouwjaar",
        "pand_identificatie",
        "pandstatus",
        "nummeraanduiding_hoofdadres_identificatie",
        "openbare_ruimte_identificatie",
        "woonplaats_identificatie",
        "bronhouder_identificatie",
    ),
    "pand": (
        "identificatie",
        "bouwjaar",
        "status",
        "gebruiksdoel",
        "oppervlakte_min",
        "oppervlakte_max",
        "aantal_verblijfsobjecten",
    ),
    "woonplaats": (
        "identificatie",
        "status",
        "woonplaats",
        "bronhouder_identificatie",
    ),
    "ligplaats": (
        "identificatie",
        "status",
        "openbare_ruimte_naam",
        "openbare_ruimte_naam_kort",
        "huisnummer",
        "huisletter",
        "toevoeging",
        "postcode",
        "woonplaats_naam",
        "nummeraanduiding_hoofdadres_identificatie",
        "openbare_ruimte_identificatie",
        "woonplaats_identificatie",
        "bronhouder_identificatie",
    ),
    "standplaats": (
        "identificatie",
        "status",
        "openbare_ruimte_naam",
        "openbare_ruimte_naam_kort",
        "huisnummer",
        "huisletter",
        "toevoeging",
        "postcode",
        "woonplaats_naam",
        "nummeraanduiding_hoofdadres_identificatie",
        "openbare_ruimte_identificatie",
        "woonplaats_identificatie",
        "bronhouder_identificatie",
    ),
}

GPKG_INT_FIELDS = {
    "aantal_verblijfsobjecten",
    "bouwjaar",
    "feature_id",
    "huisnummer",
    "oppervlakte",
    "oppervlakte_max",
    "oppervlakte_min",
}


class PDOKBAGGeoPackageSource:
    name = "bag_gpkg"
    schema_version = "v1"

    def __init__(
        self,
        run_id: str,
        source_ref: str | Path,
        layer: str = "verblijfsobject",
        max_features: int | None = None,
    ) -> None:
        if layer not in GPKG_LAYER_FIELDS:
            supported = ", ".join(sorted(GPKG_LAYER_FIELDS))
            raise ValueError(f"Unsupported BAG GPKG layer '{layer}'. Expected one of: {supported}")
        self.run_id = run_id
        self.source_ref = str(source_ref)
        self.layer = layer
        self.max_features = max_features

    def iter_records(self, since: datetime | None = None) -> Iterator[RawRecord]:
        _ = since

        df = pyogrio.read_dataframe(
            self.source_ref,
            layer=self.layer,
            columns=list(GPKG_LAYER_FIELDS[self.layer]),
            fid_as_index=True,
            max_features=self.max_features,
        )

        for feature_id_raw, row in df.iterrows():
            feature_id = self._as_int(row.get("feature_id", feature_id_raw))
            if feature_id is None:
                raise ValueError(f"BAG {self.layer} row is missing feature_id")
            identificatie = str(row.get("identificatie"))

            properties = self._properties_for_row(row, feature_id, identificatie)

            geometry = row.get("geometry")

            payload = {
                "type": "Feature",
                "id": identificatie,
                "properties": properties,
                "geometry": mapping(geometry) if geometry is not None else None,
            }

            yield RawRecord(
                source_name=self.name,
                entity_name=f"bag_gpkg.{self.layer}",
                natural_key=identificatie,
                retrieved_at=datetime.now(timezone.utc),
                run_id=self.run_id,
                payload=payload,
                schema_version=self.schema_version,
            )

    def _properties_for_row(
        self,
        row: Any,
        feature_id: int,
        identificatie: str,
    ) -> dict[str, Any]:
        properties: dict[str, Any] = {
            "feature_id": feature_id,
            "identificatie": identificatie,
        }
        for field in GPKG_LAYER_FIELDS[self.layer]:
            if field == "identificatie":
                continue
            value = row.get(field)
            properties[field] = (
                self._as_int(value) if field in GPKG_INT_FIELDS else self._clean_value(value)
            )
        return properties

    @staticmethod
    def _as_int(value: object) -> int | None:
        if PDOKBAGGeoPackageSource._is_missing(value):
            return None
        if isinstance(value, SupportsInt):
            return int(value)
        if isinstance(value, str):
            return int(value)
        raise TypeError(f"Expected int-like value, got {type(value).__name__}")

    @staticmethod
    def _clean_value(value: object) -> object | None:
        return None if PDOKBAGGeoPackageSource._is_missing(value) else value

    @staticmethod
    def _is_missing(value: object) -> bool:
        if value is None:
            return True
        if value.__class__.__name__ in {"NAType", "NaTType"}:
            return True
        if isinstance(value, float) and isnan(value):
            return True
        try:
            return bool(value != value)
        except (TypeError, ValueError):
            return False
