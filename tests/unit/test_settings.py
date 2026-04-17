from fip.settings import Settings


def test_settings_reads_lakekeeper_catalog_uri_from_explicit_env(monkeypatch) -> None:
    monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://localhost:9999/catalog")

    settings = Settings()

    assert settings.lakekeeper_catalog_uri == "http://localhost:9999/catalog"


def test_settings_reads_bronze_namespace_from_fip_prefixed_env(monkeypatch) -> None:
    monkeypatch.setenv("FIP_BRONZE_NAMESPACE", "silver")

    settings = Settings()

    assert settings.bronze_namespace == "silver"
