from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import httpx


def resolve_gpkg_source_ref(
    source_ref: str,
    cache_dir: Path,
    refresh_cache: bool = False,
) -> Path | str:
    """Return a local GeoPackage path, downloading URL sources into a cache first."""
    if not _is_url(source_ref):
        return source_ref

    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / _filename_from_url(source_ref)
    if target.exists() and target.stat().st_size > 0 and not refresh_cache:
        return target

    _download_to_path(source_ref, target)
    return target


def _is_url(source_ref: str) -> bool:
    scheme = urlparse(source_ref).scheme.lower()
    return scheme in {"http", "https"}


def _filename_from_url(url: str) -> str:
    name = Path(urlparse(url).path).name
    return name or "bag-light.gpkg"


def _download_to_path(url: str, target: Path) -> None:
    partial_target = target.with_suffix(f"{target.suffix}.part")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=120.0) as response:
            response.raise_for_status()
            with partial_target.open("wb") as handle:
                for chunk in response.iter_bytes():
                    if chunk:
                        handle.write(chunk)
        partial_target.replace(target)
    finally:
        if partial_target.exists():
            partial_target.unlink()
