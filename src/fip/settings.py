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

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.template"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
