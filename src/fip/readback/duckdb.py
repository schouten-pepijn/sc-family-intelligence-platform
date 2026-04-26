from pathlib import Path
from urllib.parse import urlsplit

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


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _duckdb_s3_endpoint(endpoint: str) -> str:
    parsed = urlsplit(endpoint)
    return parsed.netloc if parsed.netloc else endpoint


def attach_iceberg_catalog(
    conn: duckdb.DuckDBPyConnection,
    alias: str = "iceberg_catalog",
) -> None:
    settings = get_settings()
    use_ssl = "true" if settings.s3_endpoint.startswith("https://") else "false"

    conn.execute(f"""
        CREATE OR REPLACE TEMPORARY SECRET fip_rustfs (
            TYPE s3,
            KEY_ID '{_sql_string(settings.s3_access_key_id)}',
            SECRET '{_sql_string(settings.s3_secret_access_key)}',
            REGION '{_sql_string(settings.aws_region)}',
            ENDPOINT '{_sql_string(_duckdb_s3_endpoint(settings.s3_endpoint))}',
            URL_STYLE 'path',
            USE_SSL {use_ssl}
        )
        """)

    conn.execute(f"""
        CREATE OR REPLACE TEMPORARY SECRET fip_polaris (
            TYPE iceberg,
            CLIENT_ID '{_sql_string(settings.polaris_client_id)}',
            CLIENT_SECRET '{_sql_string(settings.polaris_client_secret)}',
            OAUTH2_SCOPE '{_sql_string(settings.polaris_scope)}',
            OAUTH2_SERVER_URI '{_sql_string(settings.polaris_oauth2_uri)}'
        )
        """)

    conn.execute(f"""
        ATTACH '{_sql_string(settings.polaris_catalog_name)}' AS {alias} (
            TYPE iceberg,
            ENDPOINT '{_sql_string(settings.polaris_catalog_uri)}',
            SECRET fip_polaris,
            DEFAULT_REGION '{_sql_string(settings.aws_region)}'
        )
        """)


def show_tables(
    conn: duckdb.DuckDBPyConnection,
    namespace: str | None = None,
    alias: str = "iceberg_catalog",
) -> list[tuple]:
    if namespace is None:
        namespace = get_settings().bronze_namespace

    return conn.execute(f"SHOW TABLES FROM {alias}.{namespace}").fetchall()


def count_rows(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    namespace: str | None = None,
    alias: str = "iceberg_catalog",
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
    alias: str = "iceberg_catalog",
) -> list[tuple]:
    if namespace is None:
        namespace = get_settings().bronze_namespace

    arrow_table = conn.execute(
        f"SELECT * FROM {alias}.{namespace}.{table_name} LIMIT {limit}"
    ).to_arrow_table()
    return [tuple(row.values()) for row in arrow_table.to_pylist()]
