from __future__ import annotations

import hashlib
import io
import json
import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import httpx

from fip.ingestion.base import RawRecord, Source

TABLE_NS = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
OFFICE_NS = "urn:oasis:names:tc:opendocument:xmlns:office:1.0"
TEXT_NS = "urn:oasis:names:tc:opendocument:xmlns:text:1.0"
ODS_NAMESPACES = {"table": TABLE_NS, "office": OFFICE_NS, "text": TEXT_NS}

STABLE_KEY_FIELDS = (
    "brin",
    "vestnr",
    "onderwijssoort",
    "schooljaar",
    "peildatum",
    "oordeel",
    "standaard",
    "indicator",
    "kwaliteitsgebied",
)


@dataclass(frozen=True)
class OnderwijsInspectieSource(Source):
    source_ref: str
    run_id: str

    name: str = "onderwijsinspectie"
    schema_version: str = "v2"

    def healthcheck(self) -> bool:
        try:
            self._load_records(limit=1)
            return True
        except (OSError, ValueError, httpx.HTTPError, ET.ParseError, zipfile.BadZipFile):
            return False

    def iter_records(self, since: datetime | None = None) -> Iterator[RawRecord]:
        _ = since
        for row in self._load_records():
            yield RawRecord(
                source_name=self.name,
                entity_name="inspection.school_quality",
                natural_key=self._natural_key(row),
                retrieved_at=datetime.now(timezone.utc),
                run_id=self.run_id,
                payload=row,
                schema_version=self.schema_version,
            )

    def _load_records(self, limit: int | None = None) -> list[dict[str, str]]:
        source_bytes = self._load_source_bytes()
        rows = self._parse_ods_bytes(source_bytes)
        if limit is not None:
            return rows[:limit]
        return rows

    def _load_source_bytes(self) -> bytes:
        if self._is_url(self.source_ref):
            return self._download_remote_source(self.source_ref)

        path = Path(self.source_ref)
        if not path.exists():
            raise FileNotFoundError(f"Source path does not exist: {path}")
        return path.read_bytes()

    def _download_remote_source(self, url: str) -> bytes:
        response = httpx.get(url, timeout=60, follow_redirects=True)
        response.raise_for_status()

        if self._looks_like_ods(response):
            return response.content

        if self._looks_like_html(response):
            download_url = self._extract_download_url(response.text, str(response.url))
            download = httpx.get(download_url, timeout=60, follow_redirects=True)
            download.raise_for_status()
            return download.content

        return response.content

    def _extract_download_url(self, html: str, base_url: str) -> str:
        parser = _AnchorCollector()
        parser.feed(html)

        candidates: list[tuple[int, str]] = []
        for href, text in parser.links:
            resolved_href = urljoin(base_url, href)
            score = self._download_link_score(resolved_href, text)
            if score > 0:
                candidates.append((score, resolved_href))

        if not candidates:
            raise ValueError("Could not find a downloadable onderwijsinspectie file link")

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _download_link_score(self, href: str, text: str) -> int:
        haystack = f"{href} {text}".lower()
        score = 0

        if "primair" in haystack:
            score += 4
        if "voortgezet" in haystack:
            score += 4
        if "speciaal" in haystack:
            score += 3
        if "school" in haystack:
            score += 2
        if "oordeel" in haystack:
            score += 2
        if "opendocument-spreadsheet" in haystack or href.lower().endswith(".ods"):
            score += 3
        if "download" in haystack:
            score += 1
        if "besturen" in haystack:
            score -= 3

        return score

    def _parse_ods_bytes(self, data: bytes) -> list[dict[str, str]]:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            content = archive.read("content.xml")

        root = ET.fromstring(content)
        table = root.find(".//table:table", ODS_NAMESPACES)
        if table is None:
            raise ValueError("ODS file does not contain a table")

        headers: list[str] | None = None
        rows: list[dict[str, str]] = []

        for row_element in table.findall("table:table-row", ODS_NAMESPACES):
            row_values = self._row_values(row_element)
            row_repeat = int(row_element.attrib.get(f"{{{TABLE_NS}}}number-rows-repeated", "1"))

            if not any(value.strip() for value in row_values):
                continue

            if headers is None:
                headers = self._normalize_headers(row_values)
                continue

            row = self._row_mapping(headers, row_values)
            for _ in range(row_repeat):
                rows.append(dict(row))

        if headers is None:
            raise ValueError("ODS file does not contain a header row")

        return rows

    def _row_values(self, row_element: ET.Element) -> list[str]:
        values: list[str] = []
        for cell in row_element:
            tag = self._local_name(cell.tag)
            repeat = int(cell.attrib.get(f"{{{TABLE_NS}}}number-columns-repeated", "1"))

            if tag == "covered-table-cell":
                values.extend([""] * repeat)
                continue

            if tag != "table-cell":
                continue

            cell_value = self._cell_value(cell)
            values.extend([cell_value] * repeat)

        return values

    def _cell_value(self, cell: ET.Element) -> str:
        value_type = cell.attrib.get(f"{{{OFFICE_NS}}}value-type")

        value: Any | None
        if value_type == "string":
            value = cell.attrib.get(f"{{{OFFICE_NS}}}string-value")
        elif value_type in {"float", "currency", "percentage"}:
            value = cell.attrib.get(f"{{{OFFICE_NS}}}value")
        elif value_type == "date":
            value = cell.attrib.get(f"{{{OFFICE_NS}}}date-value")
        elif value_type == "boolean":
            value = cell.attrib.get(f"{{{OFFICE_NS}}}boolean-value")
        else:
            value = None

        if value is None:
            parts = [
                "".join(paragraph.itertext()).strip()
                for paragraph in cell.findall("text:p", ODS_NAMESPACES)
            ]
            parts = [part for part in parts if part]
            if parts:
                value = "\n".join(parts)
            else:
                value = "".join(cell.itertext()).strip()

        return str(value).strip()

    def _normalize_headers(self, headers: list[str]) -> list[str]:
        normalized: list[str] = []
        counts: dict[str, int] = {}

        for index, header in enumerate(headers):
            name = re.sub(r"[^0-9a-zA-Z]+", "_", header.strip().lower()).strip("_")
            if not name:
                name = f"column_{index + 1}"

            count = counts.get(name, 0)
            counts[name] = count + 1
            if count:
                name = f"{name}_{count + 1}"
            normalized.append(name)

        return normalized

    def _row_mapping(self, headers: list[str], values: list[str]) -> dict[str, str]:
        row: dict[str, str] = {}
        for index, header in enumerate(headers):
            row[header] = values[index] if index < len(values) else ""
        return row

    def _natural_key(self, row: dict[str, str]) -> str:
        parts = [f"{field}={row[field]}" for field in STABLE_KEY_FIELDS if row.get(field)]
        if parts:
            return "|".join(parts)

        normalized_row = {key: value for key, value in sorted(row.items()) if value}
        digest = hashlib.sha1(
            json.dumps(normalized_row, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return digest

    @staticmethod
    def _is_url(value: str) -> bool:
        return value.startswith("http://") or value.startswith("https://")

    @staticmethod
    def _looks_like_html(response: httpx.Response) -> bool:
        content_type = response.headers.get("content-type", "").lower()
        return "html" in content_type or response.text.lstrip().startswith("<")

    @staticmethod
    def _looks_like_ods(response: httpx.Response) -> bool:
        content_type = response.headers.get("content-type", "").lower()
        return (
            "opendocument.spreadsheet" in content_type
            or response.url.path.lower().endswith(".ods")
            or response.content.startswith(b"PK")
        )

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]


class _AnchorCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._current_href: str | None = None
        self._current_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return

        for name, value in attrs:
            if name.lower() == "href" and value:
                self._current_href = value
                self._current_text = []
                return

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._current_href is None:
            return

        self.links.append((self._current_href, "".join(self._current_text).strip()))
        self._current_href = None
        self._current_text = []
