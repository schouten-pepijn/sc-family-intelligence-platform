from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    lakekeeper_catalog_uri: str = Field(
        default="http://localhost:8181/catalog",
        validation_alias="LAKEKEEPER_CATALOG_URI",
    )
    bronze_namespace: str = Field(
        default="bronze",
        validation_alias=AliasChoices("FIP_BRONZE_NAMESPACE", "BRONZE_NAMESPACE"),
    )
<<<<<<< Updated upstream
=======
    lakekeeper_warehouse_name: str = Field(
        default="local",
        validation_alias="LAKEKEEPER_WAREHOUSE_NAME",
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
        default="minio",
        validation_alias=AliasChoices("S3_ACCESS_KEY_ID", "MINIO_ROOT_USER"),
    )
    s3_secret_access_key: str = Field(
        default="minio123",
        validation_alias=AliasChoices("S3_SECRET_ACCESS_KEY", "MINIO_ROOT_PASSWORD"),
    )
    s3_path_style_access: bool = Field(
        default=True,
        validation_alias="S3_PATH_STYLE_ACCESS",
    )
>>>>>>> Stashed changes

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.template"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
