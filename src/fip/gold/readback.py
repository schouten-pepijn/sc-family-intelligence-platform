from __future__ import annotations

from collections.abc import Sequence

import psycopg
from psycopg.sql import Identifier, SQL

from fip.settings import get_settings


def connect() -> psycopg.Connection:
    settings = get_settings()
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )


def count_rows(
    conn: psycopg.Connection,
    table_name: str,
    schema: str | None = None,
) -> int:
    if schema is None:
        schema = get_settings().postgres_schema

    query = SQL("SELECT COUNT(*) FROM {}.{}").format(
        Identifier(schema),
        Identifier(table_name),
    )
    result = conn.execute(query).fetchone()
    if result is None:
        raise ValueError(f"No result returned for table {schema}.{table_name}")

    return int(result[0])


def sample_rows(
    conn: psycopg.Connection,
    table_name: str,
    limit: int = 5,
    schema: str | None = None,
) -> list[tuple[object, ...]]:
    if schema is None:
        schema = get_settings().postgres_schema

    query = SQL("SELECT * FROM {}.{} LIMIT %s").format(
        Identifier(schema),
        Identifier(table_name),
    )
    cursor = conn.execute(query, (limit,))
    rows: Sequence[tuple[object, ...]] = cursor.fetchall()
    return list(rows)
