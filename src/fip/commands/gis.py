from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import s3fs  # type: ignore[import-untyped]
import typer

from fip.cli import app
from fip.gold.core.service import write_rows_to_sink
from fip.gold.gis.region_geom_writer import RegionGeomLandingWriter
from fip.settings import Settings, get_settings

DEFAULT_REGION_YEAR = 2025
DEFAULT_COLLECTION = "gemeenten"
DEFAULT_SOURCE_NAME = "pdok_cbs_wijken_en_buurten"
DEFAULT_SCHEMA_VERSION = "v1"
DEFAULT_REGION_LEVEL = "municipality"
PDOK_BASE_URL = "https://api.pdok.nl"


def _build_filesystem(settings: Settings) -> s3fs.S3FileSystem:
    config_kwargs: dict[str, dict[str, str]] | None = None
    if settings.s3_path_style_access:
        config_kwargs = {"s3": {"addressing_style": "path"}}

    return s3fs.S3FileSystem(
        key=settings.s3_access_key_id,
        secret=settings.s3_secret_access_key,
        client_kwargs={
            "endpoint_url": settings.s3_endpoint,
            "region_name": settings.aws_region,
        },
        config_kwargs=config_kwargs,
        use_ssl=settings.s3_endpoint.startswith("https://"),
        use_listings_cache=False,
    )


def _is_s3_uri(uri: str) -> bool:
    return uri.startswith("s3://")


def _read_text(uri: str, filesystem: s3fs.S3FileSystem | None = None) -> str:
    if _is_s3_uri(uri):
        fs = filesystem or _build_filesystem(get_settings())
        with fs.open(uri, "r", encoding="utf-8") as handle:
            return handle.read()
    return Path(uri).read_text(encoding="utf-8")


def _write_text(uri: str, content: str, filesystem: s3fs.S3FileSystem | None = None) -> None:
    if _is_s3_uri(uri):
        fs = filesystem or _build_filesystem(get_settings())
        with fs.open(uri, "w", encoding="utf-8") as handle:
            handle.write(content)
        return

    path = Path(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _pdok_collection_url(year: int, collection: str) -> str:
    return (
        f"{PDOK_BASE_URL}/cbs/wijken-en-buurten-{year}/ogc/v1/collections/"
        f"{collection}/items?f=json&limit=1000"
    )


def _next_page_url(payload: Mapping[str, object], response: httpx.Response) -> str | None:
    links = payload.get("links")
    if isinstance(links, list):
        for link in links:
            if not isinstance(link, Mapping):
                continue
            if link.get("rel") != "next":
                continue
            href = link.get("href")
            if isinstance(href, str) and href:
                return str(httpx.URL(str(response.url)).join(href))

    next_link = response.links.get("next")
    if next_link:
        href = next_link.get("url")
        if isinstance(href, str) and href:
            return href
        if isinstance(href, bytes) and href:
            return href.decode("utf-8")

    return None


def _iter_pdok_region_features(year: int, collection: str) -> Iterator[dict[str, object]]:
    next_url: str | None = _pdok_collection_url(year, collection)
    with httpx.Client(timeout=60.0) as client:
        while next_url:
            response = client.get(next_url)
            response.raise_for_status()
            payload = response.json()
            features = payload.get("features", [])
            if not isinstance(features, list):
                raise ValueError("PDOK response did not contain a feature list")

            for feature in features:
                if not isinstance(feature, dict):
                    raise ValueError("PDOK feature payload must be an object")
                yield feature

            next_url = _next_page_url(payload, response)


def _get_property(properties: Mapping[str, object], *names: str) -> str:
    for name in names:
        value = properties.get(name)
        if value is None:
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return stripped
            continue
        return str(value)
    raise ValueError(f"Missing required property; expected one of {', '.join(names)}")


def _parse_date(value: object) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        return date.fromisoformat(value)
    raise ValueError("Expected an ISO date string")


def _normalize_region_feature(
    feature: Mapping[str, object],
    year: int,
    run_id: str,
) -> dict[str, object] | None:
    properties_raw = feature.get("properties")
    geometry = feature.get("geometry")

    if not isinstance(properties_raw, Mapping):
        raise ValueError("PDOK feature is missing a properties object")
    if geometry is None:
        raise ValueError("PDOK feature is missing geometry")

    region_id = _get_property(properties_raw, "gemeentecode", "region_id")
    region_name = _get_property(properties_raw, "gemeentenaam", "region_name")
    if region_id == "GM0998" or region_name.casefold() == "buitenland":
        return None

    source_version = (
        _get_property(properties_raw, "source_version", "jaar")
        if ("source_version" in properties_raw or "jaar" in properties_raw)
        else str(year)
    )

    valid_from = properties_raw.get("valid_from")
    if valid_from is None:
        valid_from_date = date(year, 1, 1)
    else:
        valid_from_date = _parse_date(valid_from)

    retrieved_at = properties_raw.get("retrieved_at")
    if isinstance(retrieved_at, str) and retrieved_at:
        retrieved_at_value = retrieved_at
    else:
        retrieved_at_value = datetime.now(timezone.utc).isoformat()

    schema_version = (
        _get_property(properties_raw, "schema_version")
        if "schema_version" in properties_raw
        else DEFAULT_SCHEMA_VERSION
    )
    http_status_raw = properties_raw.get("http_status", 200)
    http_status = int(http_status_raw) if not isinstance(http_status_raw, int) else http_status_raw

    return {
        "type": "Feature",
        "properties": {
            "source_name": DEFAULT_SOURCE_NAME,
            "natural_key": f"{region_id}|{source_version}",
            "retrieved_at": retrieved_at_value,
            "run_id": run_id,
            "schema_version": schema_version,
            "http_status": http_status,
            "region_id": region_id,
            "region_level": DEFAULT_REGION_LEVEL,
            "region_name": region_name,
            "source_version": source_version,
            "valid_from": valid_from_date.isoformat(),
        },
        "geometry": geometry,
    }


def _archive_region_geom(
    *,
    year: int,
    run_id: str,
    collection: str = DEFAULT_COLLECTION,
    target: str = "s3",
    output_dir: Path = Path(".raw"),
    filesystem: s3fs.S3FileSystem | None = None,
) -> str:
    features = [
        normalized
        for feature in _iter_pdok_region_features(year, collection)
        if (normalized := _normalize_region_feature(feature, year=year, run_id=run_id)) is not None
    ]
    document = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "source_name": DEFAULT_SOURCE_NAME,
            "source_year": year,
            "collection": collection,
            "run_id": run_id,
        },
    }
    content = json.dumps(document, ensure_ascii=False)

    if target == "s3":
        bucket = get_settings().s3_bucket
        uri = f"s3://{bucket}/raw/region_geom/cbs/wijken-en-buurten-{year}/{run_id}/{collection}.geojson"
    elif target == "local":
        uri = str(
            output_dir
            / "raw"
            / "region_geom"
            / f"cbs-wijken-en-buurten-{year}"
            / run_id
            / f"{collection}.geojson"
        )
    else:
        raise typer.BadParameter("target must be either 'local' or 's3'")

    _write_text(uri, content, filesystem=filesystem)
    return uri


def _build_region_geom_rows(
    input_uri: str, filesystem: s3fs.S3FileSystem | None = None
) -> list[dict[str, object]]:
    payload = json.loads(_read_text(input_uri, filesystem=filesystem))
    if not isinstance(payload, dict):
        raise ValueError("GeoJSON payload must be an object")
    if payload.get("type") != "FeatureCollection":
        raise ValueError("Expected a GeoJSON FeatureCollection")

    features = payload.get("features", [])
    if not isinstance(features, list):
        raise ValueError("FeatureCollection features must be a list")

    rows: list[dict[str, object]] = []
    for feature in features:
        if not isinstance(feature, dict):
            raise ValueError("FeatureCollection feature must be an object")
        properties_raw = feature.get("properties")
        geometry = feature.get("geometry")
        if not isinstance(properties_raw, Mapping):
            raise ValueError("Feature properties must be an object")
        if geometry is None:
            raise ValueError("Feature geometry is required")

        retrieved_at_raw = _get_property(properties_raw, "retrieved_at")
        valid_from_raw = _get_property(properties_raw, "valid_from")
        rows.append(
            {
                "source_name": _get_property(properties_raw, "source_name"),
                "natural_key": _get_property(properties_raw, "natural_key"),
                "retrieved_at": datetime.fromisoformat(retrieved_at_raw),
                "run_id": _get_property(properties_raw, "run_id"),
                "schema_version": _get_property(properties_raw, "schema_version"),
                "http_status": int(properties_raw.get("http_status", 200)),
                "region_id": _get_property(properties_raw, "region_id"),
                "region_level": _get_property(properties_raw, "region_level"),
                "region_name": _get_property(properties_raw, "region_name"),
                "source_version": _get_property(properties_raw, "source_version"),
                "valid_from": _parse_date(valid_from_raw),
                "geometry": json.dumps(geometry, ensure_ascii=False, sort_keys=True),
            }
        )

    return rows


@app.command("archive-region-geom")
def archive_region_geom(
    year: int = typer.Option(
        DEFAULT_REGION_YEAR,
        help="CBS/PDOK Wijken en Buurten dataset year to archive.",
    ),
    run_id: str = typer.Option(
        f"region-geom-{DEFAULT_REGION_YEAR}",
        help="Run identifier used in the archived S3 path.",
    ),
    collection: str = typer.Option(
        DEFAULT_COLLECTION,
        help="PDOK collection to fetch, usually gemeenten.",
    ),
    target: str = typer.Option(
        "s3",
        help="Archive target: local file or S3-compatible object storage.",
    ),
    output_dir: Path = Path(".raw"),
) -> None:
    uri = _archive_region_geom(
        year=year,
        run_id=run_id,
        collection=collection,
        target=target,
        output_dir=output_dir,
    )
    typer.echo(f"Archived region geometry to {uri}")


@app.command("build-region-geom")
def build_region_geom(
    input_uri: str = typer.Option(
        ...,
        "--input",
        help="Archived GeoJSON FeatureCollection path or S3 URI.",
    ),
    table_name: str = typer.Option(
        "region_geom",
        "--table",
        help="Landing table name to load.",
    ),
) -> None:
    rows = _build_region_geom_rows(input_uri)
    sink = RegionGeomLandingWriter(table_name=table_name)
    written = write_rows_to_sink(rows, sink)
    typer.echo(f"Wrote {written} region geometry rows")


@app.command("load-region-geom")
def load_region_geom(
    year: int = typer.Option(
        DEFAULT_REGION_YEAR,
        help="CBS/PDOK Wijken en Buurten dataset year to load.",
    ),
    run_id: str = typer.Option(
        f"region-geom-{DEFAULT_REGION_YEAR}",
        help="Run identifier used in the archive path.",
    ),
    collection: str = typer.Option(
        DEFAULT_COLLECTION,
        help="PDOK collection to fetch, usually gemeenten.",
    ),
    target: str = typer.Option(
        "s3",
        help="Archive target: local file or S3-compatible object storage.",
    ),
    output_dir: Path = Path(".raw"),
    table_name: str = typer.Option(
        "region_geom",
        "--table",
        help="Landing table name to load.",
    ),
) -> None:
    uri = _archive_region_geom(
        year=year,
        run_id=run_id,
        collection=collection,
        target=target,
        output_dir=output_dir,
    )
    rows = _build_region_geom_rows(uri)
    sink = RegionGeomLandingWriter(table_name=table_name)
    written = write_rows_to_sink(rows, sink)
    typer.echo(f"Archived region geometry to {uri}")
    typer.echo(f"Wrote {written} region geometry rows")
