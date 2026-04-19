from __future__ import annotations

from collections.abc import Sequence

import psycopg
from psycopg.sql import SQL, Identifier

from fip.settings import get_settings

GOLD_OBSERVATION_FIELDS = (
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


class GoldObservationWriter:
    """Writes denormalized observations to Postgres in the Gold layer.

    Truncates before insert to maintain idempotency; Postgres is the
    application-facing landing zone and doesn't maintain history like Iceberg.
    """

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self.last_written_rows: list[dict[str, object]] = []

    def write(self, rows: list[dict[str, object]]) -> int:
        """Write rows to Postgres, truncating first for idempotent snapshots."""
        if not rows:
            self.last_written_rows = []
            return 0

        self.last_written_rows = [self._to_gold_row(row) for row in rows]
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

    def _to_gold_row(self, row: dict[str, object]) -> dict[str, object]:
        # Field selection filters to the agreed contract, catching schema mismatches early.
        return {field: row[field] for field in GOLD_OBSERVATION_FIELDS}

    def _connect(self) -> psycopg.Connection:
        settings = get_settings()
        return psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )

    def _qualified_table_name(self) -> str:
        settings = get_settings()
        return f"{settings.postgres_schema}.{self.table_name}"

    def _ensure_table(self, conn: psycopg.Connection) -> None:
        # Schema and table are created idempotently on first write; allows offline schema
        # design in dbt without duplicating schema definitions here.
        schema_name = get_settings().postgres_schema
        table_name = self.table_name
        conn.execute(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema_name)))
        conn.execute(
            SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
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
                )
                """
            ).format(Identifier(schema_name), Identifier(table_name))
        )

    def _truncate_table(self, conn: psycopg.Connection) -> None:
        schema_name = get_settings().postgres_schema
        conn.execute(
            SQL("TRUNCATE TABLE {}.{}").format(
                Identifier(schema_name),
                Identifier(self.table_name),
            )
        )

    def _insert_rows(
        self,
        conn: psycopg.Connection,
        rows: Sequence[dict[str, object]],
    ) -> None:
        schema_name = get_settings().postgres_schema
        columns = SQL(", ").join(Identifier(field) for field in GOLD_OBSERVATION_FIELDS)
        placeholders = SQL(", ").join(SQL("%s") for _ in GOLD_OBSERVATION_FIELDS)
        sql = SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            Identifier(schema_name),
            Identifier(self.table_name),
            columns,
            placeholders,
        )
        with conn.cursor() as cur:
            cur.executemany(
                sql,
                [tuple(row[field] for field in GOLD_OBSERVATION_FIELDS) for row in rows],
            )
