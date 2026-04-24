from __future__ import annotations

from collections.abc import Sequence

from fip.gold.core.postgres import PostgresFullRefreshWriter

GEO_REGION_MAPPING_FIELDS = (
    "bag_object_id",
    "bag_object_type",
    "region_id",
    "mapping_method",
    "confidence",
    "active_from",
    "active_to",
    "run_id",
)


class BAGGeoRegionMappingLandingWriter(PostgresFullRefreshWriter):
    """Writes BAG-to-region mappings to Postgres in the landing layer."""

    def _to_row(self, row: object) -> dict[str, object]:
        mapping = row
        return {field: mapping[field] for field in GEO_REGION_MAPPING_FIELDS}  # type: ignore[index]

    def _field_names(self) -> Sequence[str]:
        return GEO_REGION_MAPPING_FIELDS

    def _table_columns_sql(self) -> str:
        return """
                    bag_object_id text NOT NULL,
                    bag_object_type text NOT NULL,
                    region_id text NOT NULL,
                    mapping_method text NOT NULL,
                    confidence double precision NOT NULL,
                    active_from timestamptz NOT NULL,
                    active_to timestamptz,
                    run_id text NOT NULL
                """
