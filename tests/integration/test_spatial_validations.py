import psycopg
import pytest

from fip.settings import get_settings

settings = get_settings()


def _get_conn():
    try:
        return psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )
    except Exception as exc:  # pragma: no cover - integration skip
        pytest.skip(f"Cannot connect to Postgres: {exc}")


@pytest.mark.integration
def test_spatial_geom_not_null_and_srid():
    """Assert `geom` is populated and uses SRID 4326."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("select count(*) from staging.stg_bag_pand_spatial where geom is null;")
        row = cur.fetchone()
        if row is None:
            pytest.fail("No result returned for null-count query; table may not exist")
        null_count = row[0]

        cur.execute(
            "select distinct st_srid(geom) "
            "from staging.stg_bag_pand_spatial "
            "where geom is not null;"
        )
        srids = {row[0] for row in cur.fetchall()}

    assert null_count == 0, f"Found {null_count} rows with null geom"
    assert srids == {4326}, f"Unexpected SRIDs found: {srids}"


@pytest.mark.integration
def test_spatial_gist_index_exists():
    """Assert a GiST index exists on the `geom` column of the spatial model."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "select indexname, indexdef "
            "from pg_indexes "
            "where schemaname = 'staging' "
            "and tablename = 'stg_bag_pand_spatial';"
        )
        indexes = cur.fetchall() or []

    # look for a gist index in the index definition
    gist_indexes = [name for name, idxdef in indexes if "gist" in idxdef.lower()]
    assert gist_indexes, f"No GiST index found for staging.stg_bag_pand_spatial (found: {indexes})"
