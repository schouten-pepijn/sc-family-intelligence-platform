from fip.settings import Settings

_HOST_S3_PROPERTY_REMOVALS = (
    "client.access-key-id",
    "client.secret-access-key",
    "client.session-token",
    "client.profile-name",
    "s3.anonymous",
    "s3.force-virtual-addressing",
    "s3.profile-name",
    "s3.session-token",
    "s3.signer",
    "s3.signer.endpoint",
    "s3.signer.uri",
)


def iceberg_s3_properties(settings: Settings) -> dict[str, str]:
    properties = {
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.endpoint": settings.s3_endpoint,
        "s3.access-key-id": settings.s3_access_key_id,
        "s3.secret-access-key": settings.s3_secret_access_key,
        "s3.region": settings.aws_region,
    }
    if not settings.s3_path_style_access:
        properties["s3.force-virtual-addressing"] = "true"

    return properties


def iceberg_catalog_properties(settings: Settings) -> dict[str, str]:
    properties = {
        "type": "rest",
        "uri": settings.polaris_catalog_uri,
        "warehouse": settings.polaris_catalog_name,
        "credential": f"{settings.polaris_client_id}:{settings.polaris_client_secret}",
        "scope": settings.polaris_scope,
        "oauth2-server-uri": settings.polaris_oauth2_uri,
    }
    properties.update(iceberg_s3_properties(settings))
    return properties


def configure_table_io_for_host(table: object, settings: Settings) -> None:
    """Force PyIceberg table IO to use local RustFS client settings.

    Polaris separates the client endpoint from its internal endpoint, but this
    keeps table IO deterministic if the catalog returns extra storage props.
    """
    io = getattr(table, "io", None)
    properties = getattr(io, "properties", None)
    if properties is not None:
        for key in _HOST_S3_PROPERTY_REMOVALS:
            properties.pop(key, None)
        properties.update(iceberg_s3_properties(settings))
        fs_by_scheme = getattr(io, "fs_by_scheme", None)
        cache_clear = getattr(fs_by_scheme, "cache_clear", None)
        if cache_clear is not None:
            cache_clear()
