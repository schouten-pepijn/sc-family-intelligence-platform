from pathlib import Path

from fip.ingestion.pdok_bag.gpkg_cache import resolve_gpkg_source_ref


def test_resolve_gpkg_source_ref_returns_local_paths_unchanged(tmp_path: Path) -> None:
    source_ref = "data/pdok-bag/bag-light.gpkg"

    resolved = resolve_gpkg_source_ref(source_ref, cache_dir=tmp_path)

    assert resolved == source_ref


def test_resolve_gpkg_source_ref_reuses_cached_url_artifact(tmp_path: Path) -> None:
    cached = tmp_path / "bag-light.gpkg"
    cached.write_bytes(b"cached")

    resolved = resolve_gpkg_source_ref(
        "https://service.pdok.nl/kadaster/bag/atom/downloads/bag-light.gpkg",
        cache_dir=tmp_path,
    )

    assert resolved == cached
    assert cached.read_bytes() == b"cached"


def test_resolve_gpkg_source_ref_downloads_url_artifact(tmp_path: Path, monkeypatch) -> None:
    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def raise_for_status(self) -> None:
            return None

        def iter_bytes(self):
            yield b"downloaded"

    calls: list[tuple[str, str]] = []

    def fake_stream(method: str, url: str, **kwargs):
        calls.append((method, url))
        return FakeResponse()

    monkeypatch.setattr("fip.ingestion.pdok_bag.gpkg_cache.httpx.stream", fake_stream)

    resolved = resolve_gpkg_source_ref(
        "https://service.pdok.nl/kadaster/bag/atom/downloads/bag-light.gpkg",
        cache_dir=tmp_path,
    )

    assert resolved == tmp_path / "bag-light.gpkg"
    assert (tmp_path / "bag-light.gpkg").read_bytes() == b"downloaded"
    assert calls == [
        (
            "GET",
            "https://service.pdok.nl/kadaster/bag/atom/downloads/bag-light.gpkg",
        )
    ]
