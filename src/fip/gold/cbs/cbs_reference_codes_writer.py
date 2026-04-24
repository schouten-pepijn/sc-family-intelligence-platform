from __future__ import annotations

import re
from collections.abc import Sequence
from typing import cast

from fip.gold.core.postgres import PostgresFullRefreshWriter
from fip.ingestion.base import RawRecord

REFERENCE_FIELDS = {
    "MeasureCodes": (
        "source_name",
        "natural_key",
        "identifier",
        "title",
        "description",
        "measure_group_id",
        "data_type",
        "unit",
        "decimals",
        "presentation_type",
        "retrieved_at",
        "run_id",
        "schema_version",
        "http_status",
    ),
    "PeriodenCodes": (
        "source_name",
        "natural_key",
        "identifier",
        "title",
        "description",
        "dimension_group_id",
        "status",
        "period_year",
        "retrieved_at",
        "run_id",
        "schema_version",
        "http_status",
    ),
    "RegioSCodes": (
        "source_name",
        "natural_key",
        "identifier",
        "title",
        "description",
        "dimension_group_id",
        "retrieved_at",
        "run_id",
        "schema_version",
        "http_status",
    ),
    "EigendomCodes": (
        "source_name",
        "natural_key",
        "identifier",
        "title",
        "description",
        "dimension_group_id",
        "retrieved_at",
        "run_id",
        "schema_version",
        "http_status",
    ),
}


def build_reference_row(record: RawRecord) -> dict[str, object]:
    payload = record.payload
    parts = record.entity_name.split(".", maxsplit=1)
    if len(parts) != 2:
        raise ValueError(
            f"Invalid CBS entity name '{record.entity_name}'. Expected 'table_id.entity_name'."
        )

    entity = parts[1]

    base = {
        "source_name": record.source_name,
        "natural_key": record.natural_key,
        "identifier": payload["Identifier"],
        "title": payload["Title"],
        "description": payload.get("Description"),
        "retrieved_at": record.retrieved_at,
        "run_id": record.run_id,
        "schema_version": record.schema_version,
        "http_status": record.http_status,
    }

    if entity == "MeasureCodes":
        return {
            **base,
            "measure_group_id": payload.get("MeasureGroupId"),
            "data_type": payload.get("DataType"),
            "unit": payload.get("Unit"),
            "decimals": payload.get("Decimals"),
            "presentation_type": payload.get("PresentationType"),
        }

    if entity == "PeriodenCodes":
        identifier = str(payload["Identifier"])
        return {
            **base,
            "dimension_group_id": payload.get("DimensionGroupId"),
            "status": payload.get("Status"),
            "period_year": (int(identifier[:4]) if re.match(r"^\d{4}", identifier) else None),
        }

    if entity == "RegioSCodes":
        return {
            **base,
            "dimension_group_id": payload.get("DimensionGroupId"),
        }

    if entity == "EigendomCodes":
        return {
            **base,
            "dimension_group_id": payload.get("DimensionGroupId"),
        }

    raise ValueError(f"Unsupported entity: {entity}")


class CBSReferenceCodeWriter(PostgresFullRefreshWriter):
    """Writes reference codes to Postgres landing tables."""

    def __init__(self, table_name: str, entity: str) -> None:
        super().__init__(table_name)
        self.entity = entity

    def write(self, rows: Sequence[object]) -> int:
        if not rows:
            self.last_written_rows = []
            return 0

        self._validate_rows(rows)
        raw_rows = [cast(RawRecord, row) for row in rows]
        self.last_written_rows = [build_reference_row(row) for row in raw_rows]
        self._validate_materialized_rows(self.last_written_rows)

        conn = self._connect()
        try:
            self._ensure_table(conn)
            self._truncate_table(conn)
            self._insert_rows(conn, self.last_written_rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return len(self.last_written_rows)

    def _validate_rows(self, rows: Sequence[object]) -> None:
        expected_suffix = f".{self.entity}"
        for row in rows:
            raw_row = cast(RawRecord, row)
            if not raw_row.entity_name.endswith(expected_suffix):
                raise ValueError(
                    f"Writer for entity '{self.entity}' cannot accept "
                    f"record '{raw_row.entity_name}'."
                )

    def _to_row(self, row: object) -> dict[str, object]:
        raise NotImplementedError("CBSReferenceCodeWriter uses build_reference_row directly")

    def _field_names(self) -> Sequence[str]:
        return REFERENCE_FIELDS[self.entity]

    def _table_columns_sql(self) -> str:
        if self.entity == "MeasureCodes":
            return """
                    source_name text NOT NULL,
                    natural_key text NOT NULL,
                    identifier text NOT NULL,
                    title text NOT NULL,
                    description text,
                    measure_group_id text,
                    data_type text,
                    unit text,
                    decimals integer,
                    presentation_type text,
                    retrieved_at timestamptz NOT NULL,
                    run_id text NOT NULL,
                    schema_version text NOT NULL,
                    http_status integer NOT NULL
                """

        if self.entity == "PeriodenCodes":
            return """
                    source_name text NOT NULL,
                    natural_key text NOT NULL,
                    identifier text NOT NULL,
                    title text NOT NULL,
                    description text,
                    dimension_group_id text,
                    status text,
                    period_year integer,
                    retrieved_at timestamptz NOT NULL,
                    run_id text NOT NULL,
                    schema_version text NOT NULL,
                    http_status integer NOT NULL
                """

        if self.entity == "RegioSCodes":
            return """
                    source_name text NOT NULL,
                    natural_key text NOT NULL,
                    identifier text NOT NULL,
                    title text NOT NULL,
                    description text,
                    dimension_group_id text,
                    retrieved_at timestamptz NOT NULL,
                    run_id text NOT NULL,
                    schema_version text NOT NULL,
                    http_status integer NOT NULL
                """

        if self.entity == "EigendomCodes":
            return """
                    source_name text NOT NULL,
                    natural_key text NOT NULL,
                    identifier text NOT NULL,
                    title text NOT NULL,
                    description text,
                    dimension_group_id text,
                    retrieved_at timestamptz NOT NULL,
                    run_id text NOT NULL,
                    schema_version text NOT NULL,
                    http_status integer NOT NULL
                """

        raise ValueError(f"Unsupported entity: {self.entity}")
