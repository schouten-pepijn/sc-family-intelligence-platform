from __future__ import annotations

from collections.abc import Sequence

from fip.gold.core.postgres import PostgresFullRefreshWriter
from fip.lakehouse.silver.pdok_bag.bag_verblijfsobject import (
    BAG_VERBLIJFSOBJECT_FIELDS,
)


class BAGVerblijfsobjectLandingWriter(PostgresFullRefreshWriter):
    """Writes BAG verblijfsobject rows to Postgres in the landing layer."""

    def _to_row(self, row: object) -> dict[str, object]:
        mapping = row
        return {field: mapping[field] for field in BAG_VERBLIJFSOBJECT_FIELDS}  # type: ignore[index]

    def _field_names(self) -> Sequence[str]:
        return BAG_VERBLIJFSOBJECT_FIELDS

    def _table_columns_sql(self) -> str:
        return """
                    source_name text NOT NULL,
                    natural_key text NOT NULL,
                    retrieved_at timestamptz NOT NULL,
                    run_id text NOT NULL,
                    schema_version text NOT NULL,
                    http_status integer NOT NULL,
                    bag_id text NOT NULL,
                    verblijfsobject_identificatie text NOT NULL,
                    hoofdadres_identificatie text,
                    postcode text,
                    huisnummer bigint,
                    huisletter text,
                    toevoeging text,
                    woonplaats_naam text,
                    openbare_ruimte_naam text,
                    gebruiksdoel text,
                    oppervlakte bigint,
                    geometry text
                """
