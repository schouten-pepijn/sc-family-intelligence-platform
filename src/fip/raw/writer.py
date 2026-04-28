from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol, cast

import s3fs  # type: ignore[import-untyped]

from fip.ingestion.base import RawRecord
from fip.settings import Settings, get_settings


class RawSnapshotWriteHandle(Protocol):
    def write(self, text: str) -> object: ...

    def close(self) -> None: ...

    def __enter__(self) -> RawSnapshotWriteHandle: ...

    def __exit__(self, exc_type: object, exc: object, tb: object) -> object: ...


def serialize_raw_record(record: RawRecord) -> str:
    return json.dumps(
        {
            "source_name": record.source_name,
            "entity_name": record.entity_name,
            "natural_key": record.natural_key,
            "retrieved_at": record.retrieved_at.isoformat(),
            "run_id": record.run_id,
            "schema_version": record.schema_version,
            "http_status": record.http_status,
            "payload": record.payload,
        },
        ensure_ascii=False,
    )


class RawSnapshotWriter:
    def __init__(self, base_dir: Path | str) -> None:
        self.base_dir = Path(base_dir)

    def write(self, records: list[RawRecord]) -> int:
        if not records:
            return 0

        self._validate_single_entity(records)
        with self.open_for_record(records[0]) as handle:
            for record in records:
                handle.write(serialize_raw_record(record))
                handle.write("\n")

        return len(records)

    def open_for_record(self, record: RawRecord) -> RawSnapshotWriteHandle:
        path = self._snapshot_path(record)
        path.parent.mkdir(parents=True, exist_ok=True)
        return cast(RawSnapshotWriteHandle, path.open("w", encoding="utf-8"))

    def _snapshot_path(self, record: RawRecord) -> Path:
        if record.source_name == "cbs_statline":
            table_id, entity = record.entity_name.split(".", maxsplit=1)
            return self.base_dir / "raw" / "cbs" / table_id / record.run_id / f"{entity}.jsonl"
        if record.source_name == "bag_pdok":
            _, entity = record.entity_name.split(".", maxsplit=1)
            return self.base_dir / "raw" / "bag_pdok" / record.run_id / f"{entity}.jsonl"
        if record.source_name == "bag_gpkg":
            _, entity = record.entity_name.split(".", maxsplit=1)
            return self.base_dir / "raw" / "bag_gpkg" / record.run_id / f"{entity}.jsonl"
        raise ValueError(f"Unknown raw source '{record.source_name}'")

    def _validate_single_entity(self, records: list[RawRecord]) -> None:
        first_entity = records[0].entity_name
        if any(record.entity_name != first_entity for record in records):
            raise ValueError("RawSnapshotWriter.write expects records for a single entity")


class S3RawSnapshotWriter:
    def __init__(
        self,
        bucket: str | None = None,
        filesystem: s3fs.S3FileSystem | None = None,
    ) -> None:
        settings = get_settings()
        self.bucket = bucket or settings.s3_bucket
        self.filesystem = filesystem or self._build_filesystem(settings)

    def write(self, records: list[RawRecord]) -> int:
        if not records:
            return 0

        self._validate_single_entity(records)
        with self.open_for_record(records[0]) as handle:
            for record in records:
                handle.write(serialize_raw_record(record))
                handle.write("\n")

        return len(records)

    def open_for_record(self, record: RawRecord) -> RawSnapshotWriteHandle:
        return self.filesystem.open(self._snapshot_path(record), "w", encoding="utf-8")

    def _build_filesystem(self, settings: Settings) -> s3fs.S3FileSystem:
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

    def _snapshot_path(self, record: RawRecord) -> str:
        if record.source_name == "cbs_statline":
            table_id, entity = record.entity_name.split(".", maxsplit=1)
            return (
                f"s3://{self.bucket}/raw/cbs/{table_id}/{record.run_id}/{entity}.jsonl"
            )
        if record.source_name == "bag_pdok":
            _, entity = record.entity_name.split(".", maxsplit=1)
            return f"s3://{self.bucket}/raw/bag_pdok/{record.run_id}/{entity}.jsonl"
        if record.source_name == "bag_gpkg":
            _, entity = record.entity_name.split(".", maxsplit=1)
            return f"s3://{self.bucket}/raw/bag_gpkg/{record.run_id}/{entity}.jsonl"
        raise ValueError(f"Unknown raw source '{record.source_name}'")

    def _validate_single_entity(self, records: list[RawRecord]) -> None:
        first_entity = records[0].entity_name
        if any(record.entity_name != first_entity for record in records):
            raise ValueError(
                "S3RawSnapshotWriter.write expects records for a single entity"
            )
