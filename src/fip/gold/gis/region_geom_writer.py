from __future__ import annotations

from collections.abc import Sequence

from fip.gold.core.postgres import PostgresFullRefreshWriter


REGION_GEOM_FIELDS = (
    "source_name",
    "natural_key",
    "retrieved_at",
    "run_id",
    "schema_version",
    "http_status",
    "region_id",
    "region_level",
    "region_name",
    "source_version",
    "valid_from",
    "geometry",
)


class RegionGeomLandingWriter(PostgresFullRefreshWriter):
    """Writes authoritative region polygons to Postgres landing."""

    def _to_row(self, row: object) -> dict[str, object]:
        mapping = row
        return {field: mapping[field] for field in REGION_GEOM_FIELDS}  # type: ignore[index]

    def _field_names(self) -> Sequence[str]:
        return REGION_GEOM_FIELDS

    def _table_columns_sql(self) -> str:
        return """
            source_name text NOT NULL,
            natural_key text NOT NULL,
            retrieved_at timestamptz NOT NULL,
            run_id text NOT NULL,
            schema_version text NOT NULL,
            http_status integer NOT NULL,
            region_id text NOT NULL,
            region_level text NOT NULL,
            region_name text NOT NULL,
            source_version text NOT NULL,
            valid_from date NOT NULL,
            geometry text NOT NULL
        """
