from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import s3fs  # type: ignore[import-untyped]

from fip.ingestion.base import RawRecord
from fip.settings import Settings, get_settings

CBS_ENTITIES = ("Observations", "MeasureCodes", "PeriodenCodes", "RegioSCodes")
CBS_EXTRA_ENTITIES_BY_TABLE_ID = {
    "85036NED": ("EigendomCodes",),
    "83648NED": ("SoortMisdrijfCodes",),
}


def cbs_entities_for_table(table_id: str) -> tuple[str, ...]:
    return CBS_ENTITIES + CBS_EXTRA_ENTITIES_BY_TABLE_ID.get(table_id, ())


def deserialize_raw_record(line: str) -> RawRecord:
    data = json.loads(line)
    return RawRecord(
        source_name=data["source_name"],
        entity_name=data["entity_name"],
        natural_key=data["natural_key"],
        retrieved_at=datetime.fromisoformat(data["retrieved_at"]),
        run_id=data["run_id"],
        payload=data["payload"],
        schema_version=data["schema_version"],
        http_status=data.get("http_status", 200),
    )


class RawSnapshotReader:
    def __init__(self, base_dir: Path | str) -> None:
        self.base_dir = Path(base_dir)

    def iter_cbs_records(self, table_id: str, run_id: str) -> Iterator[RawRecord]:
        for entity in cbs_entities_for_table(table_id):
            yield from self.iter_cbs_entity_records(table_id=table_id, run_id=run_id, entity=entity)

    def iter_cbs_entity_records(
        self,
        table_id: str,
        run_id: str,
        entity: str,
    ) -> Iterator[RawRecord]:
        path = self.base_dir / "raw" / "cbs" / table_id / run_id / f"{entity}.jsonl"
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield deserialize_raw_record(line)

    def iter_bag_records(self, run_id: str, collection: str) -> Iterator[RawRecord]:
        path = self.base_dir / "raw" / "bag_pdok" / run_id / f"{collection}.jsonl"
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield deserialize_raw_record(line)

    def iter_bag_gpkg_records(self, run_id: str, layer: str) -> Iterator[RawRecord]:
        path = self.base_dir / "raw" / "bag_gpkg" / run_id / f"{layer}.jsonl"
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield deserialize_raw_record(line)


class S3RawSnapshotReader:
    def __init__(
        self,
        bucket: str | None = None,
        filesystem: s3fs.S3FileSystem | None = None,
    ) -> None:
        settings = get_settings()
        self.bucket = bucket or settings.s3_bucket
        self.filesystem = filesystem or self._build_filesystem(settings)

    def iter_cbs_records(self, table_id: str, run_id: str) -> Iterator[RawRecord]:
        for entity in cbs_entities_for_table(table_id):
            yield from self.iter_cbs_entity_records(table_id=table_id, run_id=run_id, entity=entity)

    def iter_cbs_entity_records(
        self,
        table_id: str,
        run_id: str,
        entity: str,
    ) -> Iterator[RawRecord]:
        path = f"s3://{self.bucket}/raw/cbs/{table_id}/{run_id}/{entity}.jsonl"
        with self.filesystem.open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield deserialize_raw_record(line)

    def iter_bag_records(self, run_id: str, collection: str) -> Iterator[RawRecord]:
        path = f"s3://{self.bucket}/raw/bag_pdok/{run_id}/{collection}.jsonl"
        with self.filesystem.open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield deserialize_raw_record(line)

    def iter_bag_gpkg_records(self, run_id: str, layer: str) -> Iterator[RawRecord]:
        path = f"s3://{self.bucket}/raw/bag_gpkg/{run_id}/{layer}.jsonl"
        with self.filesystem.open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield deserialize_raw_record(line)

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
