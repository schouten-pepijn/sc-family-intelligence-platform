from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    polaris_catalog_uri: str = Field(
        default="http://localhost:8181/api/catalog",
        validation_alias="POLARIS_CATALOG_URI",
    )
    polaris_oauth2_uri: str = Field(
        default="http://localhost:8181/api/catalog/v1/oauth/tokens",
        validation_alias="POLARIS_OAUTH2_URI",
    )
    bronze_namespace: str = Field(
        default="bronze",
        validation_alias=AliasChoices("FIP_BRONZE_NAMESPACE", "BRONZE_NAMESPACE"),
    )
    silver_namespace: str = Field(
        default="silver",
        validation_alias=AliasChoices("FIP_SILVER_NAMESPACE", "SILVER_NAMESPACE"),
    )
    polaris_catalog_name: str = Field(
        default="fip_catalog",
        validation_alias="POLARIS_CATALOG_NAME",
    )
    polaris_client_id: str = Field(
        default="root",
        validation_alias="POLARIS_CLIENT_ID",
    )
    polaris_client_secret: str = Field(
        default="s3cr3t",
        validation_alias="POLARIS_CLIENT_SECRET",
    )
    polaris_scope: str = Field(
        default="PRINCIPAL_ROLE:ALL",
        validation_alias="POLARIS_SCOPE",
    )
    aws_region: str = Field(
        default="local-01",
        validation_alias="AWS_REGION",
    )
    s3_endpoint: str = Field(
        default="http://localhost:9000",
        validation_alias="S3_ENDPOINT",
    )
    s3_access_key_id: str = Field(
        default="rustfsadmin",
        validation_alias=AliasChoices("S3_ACCESS_KEY_ID", "RUSTFS_ACCESS_KEY"),
    )
    s3_secret_access_key: str = Field(
        default="rustfsadmin",
        validation_alias=AliasChoices("S3_SECRET_ACCESS_KEY", "RUSTFS_SECRET_KEY"),
    )
    s3_path_style_access: bool = Field(
        default=True,
        validation_alias="S3_PATH_STYLE_ACCESS",
    )
    s3_bucket: str = Field(
        default="fip-lakehouse",
        validation_alias="S3_BUCKET",
    )
    duckdb_path: str = Field(
        default=".duckdb/fip.duckdb",
        validation_alias="DUCKDB_PATH",
    )
    postgres_host: str = Field(
        default="localhost",
        validation_alias="POSTGRES_HOST",
    )
    postgres_port: int = Field(
        default=55432,
        validation_alias="POSTGRES_PORT",
    )
    postgres_db: str = Field(
        default="fip",
        validation_alias="POSTGRES_DB",
    )
    postgres_user: str = Field(
        default="fip",
        validation_alias="POSTGRES_USER",
    )
    postgres_password: str = Field(
        default="fip123",
        validation_alias="POSTGRES_PASSWORD",
    )
    postgres_schema: str = Field(
        default="landing",
        validation_alias="POSTGRES_SCHEMA",
    )

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.template"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
