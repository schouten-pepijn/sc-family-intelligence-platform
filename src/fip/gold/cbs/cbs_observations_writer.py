from __future__ import annotations

from collections.abc import Sequence

from fip.gold.core.postgres import PostgresFullRefreshWriter

CBS_OBSERVATION_FIELDS = (
    "source_name",
    "natural_key",
    "retrieved_at",
    "run_id",
    "schema_version",
    "http_status",
    "observation_id",
    "measure_code",
    "period_code",
    "region_code",
    "numeric_value",
    "value_attribute",
    "string_value",
)


class CBSObservationLandingWriter(PostgresFullRefreshWriter):
    """Writes denormalized CBS observations to Postgres in the landing layer."""

    def _to_row(self, row: object) -> dict[str, object]:
        mapping = row
        return {field: mapping[field] for field in CBS_OBSERVATION_FIELDS}  # type: ignore[index]

    def _field_names(self) -> Sequence[str]:
        return CBS_OBSERVATION_FIELDS

    def _table_columns_sql(self) -> str:
        return """
                    source_name text NOT NULL,
                    natural_key text NOT NULL,
                    retrieved_at timestamptz NOT NULL,
                    run_id text NOT NULL,
                    schema_version text NOT NULL,
                    http_status integer NOT NULL,
                    observation_id bigint NOT NULL,
                    measure_code text NOT NULL,
                    period_code text NOT NULL,
                    region_code text NOT NULL,
                    numeric_value double precision,
                    value_attribute text,
                    string_value text
                """
