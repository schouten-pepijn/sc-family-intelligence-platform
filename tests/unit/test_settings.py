from fip.settings import Settings


def test_settings_reads_lakekeeper_catalog_uri_from_explicit_env(monkeypatch) -> None:
    monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://localhost:9999/catalog")

    settings = Settings()

    assert settings.lakekeeper_catalog_uri == "http://localhost:9999/catalog"


def test_settings_reads_bronze_namespace_from_fip_prefixed_env(monkeypatch) -> None:
    monkeypatch.setenv("FIP_BRONZE_NAMESPACE", "silver")

    settings = Settings()

    assert settings.bronze_namespace == "silver"


def test_settings_reads_silver_namespace_from_fip_prefixed_env(monkeypatch) -> None:
    monkeypatch.setenv("FIP_SILVER_NAMESPACE", "gold-silver")

    settings = Settings()

    assert settings.silver_namespace == "gold-silver"


def test_settings_reads_lakekeeper_warehouse_name_from_env(monkeypatch) -> None:
    monkeypatch.setenv("LAKEKEEPER_WAREHOUSE_NAME", "demo")

    settings = Settings()

    assert settings.lakekeeper_warehouse_name == "demo"


def test_settings_reads_minio_credentials_via_existing_env_names(monkeypatch) -> None:
    monkeypatch.setenv("MINIO_ROOT_USER", "test-user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "test-pass")

    settings = Settings()

    assert settings.s3_access_key_id == "test-user"
    assert settings.s3_secret_access_key == "test-pass"


def test_settings_reads_s3_bucket_from_env(monkeypatch) -> None:
    monkeypatch.setenv("S3_BUCKET", "raw-bucket")

    settings = Settings()

    assert settings.s3_bucket == "raw-bucket"


def test_settings_reads_postgres_defaults_from_env(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PORT", "55433")
    monkeypatch.setenv("POSTGRES_DB", "gold")
    monkeypatch.setenv("POSTGRES_USER", "gold-user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "gold-pass")
    monkeypatch.setenv("POSTGRES_SCHEMA", "analytics")

    settings = Settings()

    assert settings.postgres_host == "db"
    assert settings.postgres_port == 55433
    assert settings.postgres_db == "gold"
    assert settings.postgres_user == "gold-user"
    assert settings.postgres_password == "gold-pass"
    assert settings.postgres_schema == "analytics"


def test_settings_defaults_postgres_schema_to_landing(monkeypatch) -> None:
    monkeypatch.delenv("POSTGRES_SCHEMA", raising=False)

    settings = Settings()

    assert settings.postgres_schema == "landing"
