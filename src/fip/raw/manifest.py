from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import s3fs  # type: ignore[import-untyped]

from fip.settings import Settings, get_settings


@dataclass(frozen=True)
class SourceRunManifest:
    source_name: str
    source_family: str
    run_id: str
    started_at: datetime
    finished_at: datetime | None
    source_url: str
    source_version: str | None
    license: str
    attribution: str
    raw_uri: str
    row_count: int
    status: str
    error_message: str | None = None
    checksum: str | None = None
    file_size_bytes: int | None = None

    def to_json(self) -> str:
        return json.dumps(
            {
                **asdict(self),
                "started_at": self.started_at.isoformat(),
                "finished_at": (
                    self.finished_at.isoformat() if self.finished_at is not None else None
                ),
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, text: str) -> "SourceRunManifest":
        data: dict[str, Any] = json.loads(text)
        return cls(
            **{
                **data,
                "started_at": datetime.fromisoformat(data["started_at"]),
                "finished_at": (
                    datetime.fromisoformat(data["finished_at"])
                    if data["finished_at"] is not None
                    else None
                ),
            }
        )


class LocalManifestWriter:
    def __init__(self, base_dir: Path | str) -> None:
        self.base_dir = Path(base_dir)

    def write(self, manifest: SourceRunManifest, table_id: str | None = None) -> Path:
        path = local_manifest_path(
            base_dir=self.base_dir,
            source_name=manifest.source_name,
            run_id=manifest.run_id,
            table_id=table_id,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(manifest.to_json() + "\n", encoding="utf-8")
        return path


class S3ManifestWriter:
    def __init__(
        self,
        bucket: str | None = None,
        filesystem: s3fs.S3FileSystem | None = None,
    ) -> None:
        settings = get_settings()
        self.bucket = bucket or settings.s3_bucket
        self.filesystem = filesystem or self._build_filesystem(settings)

    def write(self, manifest: SourceRunManifest, table_id: str | None = None) -> str:
        uri = s3_manifest_uri(
            bucket=self.bucket,
            source_name=manifest.source_name,
            run_id=manifest.run_id,
            table_id=table_id,
        )
        with self.filesystem.open(uri, "w", encoding="utf-8") as handle:
            handle.write(manifest.to_json())
            handle.write("\n")
        return uri

    def _build_filesystem(self, settings: Settings) -> s3fs.S3FileSystem:
        config_kwargs = None
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


def local_manifest_path(
    base_dir: Path, source_name: str, run_id: str, table_id: str | None = None
) -> Path:
    if source_name == "cbs_statline":
        if table_id is None:
            raise ValueError("table_id is required for CBS manifests")
        return base_dir / "raw" / "cbs" / table_id / run_id / "manifest.json"

    if source_name == "bag_pdok":
        return base_dir / "raw" / "bag_pdok" / run_id / "manifest.json"

    if source_name == "bag_gpkg":
        return base_dir / "raw" / "bag_gpkg" / run_id / "manifest.json"

    raise ValueError(f"Unknown raw source '{source_name}'")


def s3_manifest_uri(bucket: str, source_name: str, run_id: str, table_id: str | None = None) -> str:
    if source_name == "cbs_statline":
        if table_id is None:
            raise ValueError("table_id is required for CBS manifests")
        return f"s3://{bucket}/raw/cbs/{table_id}/{run_id}/manifest.json"

    if source_name == "bag_pdok":
        return f"s3://{bucket}/raw/bag_pdok/{run_id}/manifest.json"

    if source_name == "bag_gpkg":
        return f"s3://{bucket}/raw/bag_gpkg/{run_id}/manifest.json"

    raise ValueError(f"Unknown raw source '{source_name}'")
