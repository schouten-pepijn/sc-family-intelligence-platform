from __future__ import annotations

from collections.abc import Sequence

import psycopg
from psycopg.sql import SQL, Identifier

from fip.lakehouse.silver.bag_pand import BAG_PAND_FIELDS
from fip.settings import get_settings


class BAGPandLandingWriter:
    """Writes BAG pand rows to Postgres in the landing layer."""

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self.last_written_rows: list[dict[str, object]] = []

    def write(self, rows: list[dict[str, object]]) -> int:
        if not rows:
            self.last_written_rows = []
            return 0

        self.last_written_rows = [self._to_landing_row(row) for row in rows]
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

    def _to_landing_row(self, row: dict[str, object]) -> dict[str, object]:
        return {field: row[field] for field in BAG_PAND_FIELDS}

    def _connect(self) -> psycopg.Connection:
        settings = get_settings()
        return psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )

    def _ensure_table(self, conn: psycopg.Connection) -> None:
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
                    bag_id text NOT NULL,
                    pand_identificatie text NOT NULL,
                    pand_status text,
                    oorspronkelijk_bouwjaar bigint,
                    geconstateerd text,
                    documentdatum text,
                    documentnummer text,
                    geometry text
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
        columns = SQL(", ").join(Identifier(field) for field in BAG_PAND_FIELDS)
        placeholders = SQL(", ").join(SQL("%s") for _ in BAG_PAND_FIELDS)
        sql = SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            Identifier(schema_name),
            Identifier(self.table_name),
            columns,
            placeholders,
        )
        with conn.cursor() as cur:
            cur.executemany(
                sql,
                [tuple(row[field] for field in BAG_PAND_FIELDS) for row in rows],
            )
