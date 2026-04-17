import duckdb

from fip.settings import get_settings


def connect() -> duckdb.DuckDBPyConnection:
    settings = get_settings()
    conn = duckdb.connect(settings.duckdb_path)
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
    namespace: str = "bronze",
    alias: str = "lakekeeper_catalog",
) -> list[tuple]:
    return conn.execute(f"SHOW TABLES FROM {alias}.{namespace}").fetchall()


def count_rows(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    namespace: str = "bronze",
    alias: str = "lakekeeper_catalog",
) -> int:
    result = conn.execute(f"SELECT COUNT(*) FROM {alias}.{namespace}.{table_name}").fetchone()

    if result is None:
        raise ValueError(f"No result returned for table {alias}.{namespace}.{table_name}")

    return int(result[0])


def sample_rows(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    limit: int = 5,
    namespace: str = "bronze",
    alias: str = "lakekeeper_catalog",
) -> list[tuple]:
    return conn.execute(f"SELECT * FROM {alias}.{namespace}.{table_name} LIMIT {limit}").fetchall()
