from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from math import isnan
from typing import Any, SupportsInt

import pyogrio  # type: ignore[import-untyped]
from shapely.geometry import mapping  # type: ignore[import-untyped]

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

        df = pyogrio.read_dataframe(self.source_ref, layer=self.layer, fid_as_index=True)

        for feature_id_raw, row in df.iterrows():
            feature_id = self._as_int(row.get("feature_id", feature_id_raw))
            if feature_id is None:
                raise ValueError("BAG verblijfsobject row is missing feature_id")
            identificatie = str(row.get("identificatie"))

            properties: dict[str, Any] = {
                "feature_id": feature_id,
                "identificatie": identificatie,
                "oppervlakte": self._as_int(row.get("oppervlakte")),
                "status": row.get("status"),
                "gebruiksdoel": row.get("gebruiksdoel"),
                "openbare_ruimte_naam": row.get("openbare_ruimte_naam"),
                "openbare_ruimte_naam_kort": row.get("openbare_ruimte_naam_kort"),
                "huisnummer": self._as_int(row.get("huisnummer")),
                "huisletter": row.get("huisletter"),
                "toevoeging": row.get("toevoeging"),
                "postcode": row.get("postcode"),
                "woonplaats_naam": row.get("woonplaats_naam"),
                "bouwjaar": self._as_int(row.get("bouwjaar")),
                "pand_identificatie": row.get("pand_identificatie"),
                "pandstatus": row.get("pandstatus"),
                "nummeraanduiding_hoofdadres_identificatie": row.get(
                    "nummeraanduiding_hoofdadres_identificatie"
                ),
                "openbare_ruimte_identificatie": row.get("openbare_ruimte_identificatie"),
                "woonplaats_identificatie": row.get("woonplaats_identificatie"),
                "bronhouder_identificatie": row.get("bronhouder_identificatie"),
            }

            geometry = row.get("geometry")

            payload = {
                "type": "Feature",
                "id": identificatie,
                "properties": properties,
                "geometry": mapping(geometry) if geometry is not None else None,
            }

            yield RawRecord(
                source_name=self.name,
                entity_name="bag_gpkg.verblijfsobject",
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
        if isinstance(value, float) and isnan(value):
            return None
        if isinstance(value, SupportsInt):
            return int(value)
        if isinstance(value, str):
            return int(value)
        raise TypeError(f"Expected int-like value, got {type(value).__name__}")
