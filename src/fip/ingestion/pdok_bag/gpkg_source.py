from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyogrio
from shapely.geometry import mapping

from fip.ingestion.base import RawRecord

GPKG_URL = "https://service.pdok.nl/kadaster/bag/atom/downloads/bag-light.gpkg"


class PDOKBAGGeoPackageSource:
    name = "bag_gpkg"
    schema_version = "v1"

    def __init__(
        self,
        run_id: str,
        source_ref: str | Path,
        layer: str = "verblijfsobject",
    ) -> None:
        if layer != "verblijfsobject":
            raise ValueError("First v2 step only supports verblijfsobject")
        self.run_id = run_id
        self.source_ref = str(source_ref)
        self.layer = layer

    def iter_records(self, since: datetime | None = None) -> Iterator[RawRecord]:
        _ = since

        df = pyogrio.read_dataframe(self.source_ref, layer=self.layer)

        for row in df.itertuples(index=False):
            feature_id = self._as_int(getattr(row, "feature_id"))
            if feature_id is None:
                raise ValueError("BAG verblijfsobject row is missing feature_id")
            identificatie = str(getattr(row, "identificatie"))

            properties: dict[str, Any] = {
                "feature_id": feature_id,
                "identificatie": identificatie,
                "oppervlakte": self._as_int(getattr(row, "oppervlakte")),
                "status": getattr(row, "status"),
                "gebruiksdoel": getattr(row, "gebruiksdoel"),
                "openbare_ruimte_naam": getattr(row, "openbare_ruimte_naam"),
                "openbare_ruimte_naam_kort": getattr(row, "openbare_ruimte_naam_kort"),
                "huisnummer": self._as_int(getattr(row, "huisnummer")),
                "huisletter": getattr(row, "huisletter"),
                "toevoeging": getattr(row, "toevoeging"),
                "postcode": getattr(row, "postcode"),
                "woonplaats_naam": getattr(row, "woonplaats_naam"),
                "bouwjaar": self._as_int(getattr(row, "bouwjaar")),
                "pand_identificatie": getattr(row, "pand_identificatie"),
                "pandstatus": getattr(row, "pandstatus"),
                "nummeraanduiding_hoofdadres_identificatie": getattr(
                    row, "nummeraanduiding_hoofdadres_identificatie"
                ),
                "openbare_ruimte_identificatie": getattr(
                    row, "openbare_ruimte_identificatie"
                ),
                "woonplaats_identificatie": getattr(row, "woonplaats_identificatie"),
                "bronhouder_identificatie": getattr(row, "bronhouder_identificatie"),
            }

            geometry = getattr(row, "geometry", None)

            payload = {
                "type": "Feature",
                "id": identificatie,
                "properties": properties,
                "geometry": mapping(geometry) if geometry is not None else None,
            }

            yield RawRecord(
                source_name=self.name,
                entity_name="bag.verblijfsobject",
                natural_key=identificatie,
                retrieved_at=datetime.now(timezone.utc),
                run_id=self.run_id,
                payload=payload,
                schema_version=self.schema_version,
            )

    @staticmethod
    def _as_int(value: object) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            return int(value)
        raise TypeError(f"Expected int-like value, got {type(value).__name__}")
