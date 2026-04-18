from pathlib import Path

import duckdb

from fip.settings import get_settings


def connect() -> duckdb.DuckDBPyConnection:
    settings = get_settings()
    duckdb_path = Path(settings.duckdb_path)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))
    return conn


def load_extensions(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute("INSTALL iceberg")
    conn.execute("LOAD iceberg")


def attach_lakekeeper_catalog(
    conn: duckdb.DuckDBPyConnection,
    alias: str = "lakekeeper_catalog",
) -> None:
    settings = get_settings()

    conn.execute(
        f"""
        ATTACH '{settings.lakekeeper_warehouse_name}' AS {alias} (
            TYPE iceberg,
            ENDPOINT '{settings.lakekeeper_catalog_uri}',
            AUTHORIZATION_TYPE 'none'
        )
        """
    )


def show_tables(
    conn: duckdb.DuckDBPyConnection,
    namespace: str | None = None,
    alias: str = "lakekeeper_catalog",
) -> list[tuple]:
    if namespace is None:
        namespace = get_settings().bronze_namespace

    return conn.execute(f"SHOW TABLES FROM {alias}.{namespace}").fetchall()


def count_rows(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    namespace: str | None = None,
    alias: str = "lakekeeper_catalog",
) -> int:
    if namespace is None:
        namespace = get_settings().bronze_namespace

    result = conn.execute(f"SELECT COUNT(*) FROM {alias}.{namespace}.{table_name}").fetchone()

    if result is None:
        raise ValueError(f"No result returned for table {alias}.{namespace}.{table_name}")

    return int(result[0])


def sample_rows(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    limit: int = 5,
    namespace: str | None = None,
    alias: str = "lakekeeper_catalog",
) -> list[tuple]:
    if namespace is None:
        namespace = get_settings().bronze_namespace

    arrow_table = conn.execute(
        f"SELECT * FROM {alias}.{namespace}.{table_name} LIMIT {limit}"
    ).fetch_arrow_table()
    return [tuple(row.values()) for row in arrow_table.to_pylist()]
