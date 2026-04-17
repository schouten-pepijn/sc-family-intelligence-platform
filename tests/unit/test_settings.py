from fip.settings import Settings


def test_settings_reads_lakekeeper_catalog_uri_from_explicit_env(monkeypatch) -> None:
    monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://localhost:9999/catalog")

    settings = Settings()

    assert settings.lakekeeper_catalog_uri == "http://localhost:9999/catalog"


def test_settings_reads_bronze_namespace_from_fip_prefixed_env(monkeypatch) -> None:
    monkeypatch.setenv("FIP_BRONZE_NAMESPACE", "silver")

    settings = Settings()

    assert settings.bronze_namespace == "silver"
<<<<<<< Updated upstream
=======


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
>>>>>>> Stashed changes
