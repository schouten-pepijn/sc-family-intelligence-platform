from __future__ import annotations

import re
from collections.abc import Sequence

import psycopg
from psycopg.sql import SQL, Identifier

from fip.ingestion.base import RawRecord
from fip.settings import get_settings

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
}


def build_reference_row(record: RawRecord) -> dict[str, object]:
    """Extract and transform reference code data from raw CBS records.

    Entity-specific logic derives values (e.g., period_year from identifier),
    compensating for missing fields in the source payload.
    """
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

    raise ValueError(f"Unsupported entity: {entity}")


class ReferenceCodeWriter:
    """Writes reference codes (measures, periods, regions) to Postgres landing tables.

    Entity-specific schema ensures type safety and documents expected fields;
    truncate-then-insert maintains idempotency across replays.
    """
    def __init__(
        self,
        table_name: str,
        entity: str,
    ) -> None:
        self.table_name = table_name
        self.entity = entity
        self.last_written_rows: list[dict[str, object]] = []

    def write(self, rows: Sequence[RawRecord]) -> int:
        """Validate, transform, and write reference codes, truncating first."""
        if not rows:
            self.last_written_rows = []
            return 0

        self._validate_rows(rows)
        self.last_written_rows = [build_reference_row(row) for row in rows]
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

    def _connect(self) -> psycopg.Connection:
        settings = get_settings()
        return psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )

    def _validate_rows(self, rows: Sequence[RawRecord]) -> None:
        # Entity name suffix check prevents silently writing wrong entity type,
        # catching configuration or pipeline logic errors early.
        expected_suffix = f".{self.entity}"
        for row in rows:
            if not row.entity_name.endswith(expected_suffix):
                raise ValueError(
                    f"Writer for entity '{self.entity}' cannot accept record '{row.entity_name}'."
                )

    def _ensure_table(self, conn: psycopg.Connection) -> None:
        schema = get_settings().postgres_schema
        conn.execute(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema)))

        if self.entity == "MeasureCodes":
            conn.execute(
                SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
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
                    )
                    """
                ).format(Identifier(schema), Identifier(self.table_name))
            )
        elif self.entity == "PeriodenCodes":
            conn.execute(
                SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
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
                    )
                    """
                ).format(Identifier(schema), Identifier(self.table_name))
            )
        elif self.entity == "RegioSCodes":
            conn.execute(
                SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
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
                    )
                    """
                ).format(Identifier(schema), Identifier(self.table_name))
            )
        else:
            raise ValueError(f"Unsupported entity: {self.entity}")

    def _truncate_table(self, conn: psycopg.Connection) -> None:
        schema = get_settings().postgres_schema
        conn.execute(
            SQL("TRUNCATE TABLE {}.{}").format(
                Identifier(schema),
                Identifier(self.table_name),
            )
        )

    def _insert_rows(
        self,
        conn: psycopg.Connection,
        rows: Sequence[dict[str, object]],
    ) -> None:
        schema = get_settings().postgres_schema
        fields = REFERENCE_FIELDS[self.entity]
        columns = SQL(", ").join(Identifier(field) for field in fields)
        placeholders = SQL(", ").join(SQL("%s") for _ in fields)
        sql = SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            Identifier(schema),
            Identifier(self.table_name),
            columns,
            placeholders,
        )
        with conn.cursor() as cur:
            cur.executemany(
                sql,
                [tuple(row[field] for field in fields) for row in rows],
            )
