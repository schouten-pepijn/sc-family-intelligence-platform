from __future__ import annotations

from collections.abc import Sequence

import psycopg
from psycopg.sql import SQL, Identifier

from fip.raw.manifest import SourceRunManifest
from fip.settings import get_settings


class SourceRunLandingWriter:
    def __init__(self, table_name: str = "source_runs") -> None:
        self.table_name = table_name
        self.last_written_rows: list[dict[str, object]] = []

    def write(self, manifests: Sequence[SourceRunManifest]) -> int:
        if not manifests:
            self.last_written_rows = []
            return 0

        rows = [self._to_row(manifest) for manifest in manifests]
        self.last_written_rows = rows

        conn = self._connect()
        try:
            self._ensure_table(conn)
            self._upsert_rows(conn, rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return len(rows)

    def _to_row(self, manifest: SourceRunManifest) -> dict[str, object]:
        return {
            "source_name": manifest.source_name,
            "source_family": manifest.source_family,
            "run_id": manifest.run_id,
            "started_at": manifest.started_at,
            "finished_at": manifest.finished_at,
            "source_url": manifest.source_url,
            "source_version": manifest.source_version,
            "license": manifest.license,
            "attribution": manifest.attribution,
            "raw_uri": manifest.raw_uri,
            "row_count": manifest.row_count,
            "status": manifest.status,
            "error_message": manifest.error_message,
            "checksum": manifest.checksum,
            "file_size_bytes": manifest.file_size_bytes,
        }

    def _connect(self) -> psycopg.Connection:
        settings = get_settings()
        return psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )

    def _ensure_table(self, conn: psycopg.Connection) -> None:
        schema_name = get_settings().postgres_schema
        conn.execute(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema_name)))
        conn.execute(
            SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
                    source_name text NOT NULL,
                    source_family text NOT NULL,
                    run_id text NOT NULL,
                    started_at timestamptz NOT NULL,
                    finished_at timestamptz,
                    source_url text NOT NULL,
                    source_version text,
                    license text NOT NULL,
                    attribution text NOT NULL,
                    raw_uri text NOT NULL,
                    row_count bigint NOT NULL,
                    status text NOT NULL,
                    error_message text,
                    checksum text,
                    file_size_bytes bigint,
                    PRIMARY KEY (source_name, run_id)
                )
            """).format(Identifier(schema_name), Identifier(self.table_name))
        )

    def _upsert_rows(self, conn: psycopg.Connection, rows: list[dict[str, object]]) -> None:
        schema_name = get_settings().postgres_schema
        fields = [
            "source_name",
            "source_family",
            "run_id",
            "started_at",
            "finished_at",
            "source_url",
            "source_version",
            "license",
            "attribution",
            "raw_uri",
            "row_count",
            "status",
            "error_message",
            "checksum",
            "file_size_bytes",
        ]
        columns = SQL(", ").join(Identifier(field) for field in fields)
        placeholders = SQL(", ").join(SQL("%s") for _ in fields)
        updates = SQL(", ").join(
            SQL("{} = EXCLUDED.{}").format(Identifier(field), Identifier(field))
            for field in fields
            if field not in {"source_name", "run_id"}
        )

        sql = SQL("""
            INSERT INTO {}.{} ({})
            VALUES ({})
            ON CONFLICT (source_name, run_id)
            DO UPDATE SET {}
        """).format(
            Identifier(schema_name),
            Identifier(self.table_name),
            columns,
            placeholders,
            updates,
        )

        with conn.cursor() as cur:
            cur.executemany(sql, [tuple(row[field] for field in fields) for row in rows])
