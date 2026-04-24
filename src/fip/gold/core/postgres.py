from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

import psycopg
from psycopg.sql import SQL, Identifier

from fip.settings import get_settings


class PostgresFullRefreshWriter(ABC):
    """Shared Postgres landing writer with truncate-then-insert semantics."""

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self.last_written_rows: list[dict[str, object]] = []

    def write(self, rows: Sequence[object]) -> int:
        if not rows:
            self.last_written_rows = []
            return 0

        self._validate_rows(rows)
        self.last_written_rows = [self._to_row(row) for row in rows]
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

    def _validate_materialized_rows(self, rows: Sequence[dict[str, object]]) -> None:
        run_ids = {str(row["run_id"]) for row in rows}
        if len(run_ids) != 1:
            raise ValueError(
                f"{self.__class__.__name__} expects a single run_id per writegot {sorted(run_ids)}"
            )

    def _validate_rows(self, rows: Sequence[object]) -> None:
        return None

    @abstractmethod
    def _to_row(self, row: object) -> dict[str, object]: ...

    @abstractmethod
    def _field_names(self) -> Sequence[str]: ...

    @abstractmethod
    def _table_columns_sql(self) -> str: ...

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
        conn.execute(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema_name)))
        conn.execute(
            SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                Identifier(schema_name),
                Identifier(self.table_name),
                SQL(self._table_columns_sql()),
            )
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
        fields = self._field_names()
        columns = SQL(", ").join(Identifier(field) for field in fields)
        placeholders = SQL(", ").join(SQL("%s") for _ in fields)
        sql = SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            Identifier(schema_name),
            Identifier(self.table_name),
            columns,
            placeholders,
        )
        with conn.cursor() as cur:
            cur.executemany(sql, [tuple(row[field] for field in fields) for row in rows])
