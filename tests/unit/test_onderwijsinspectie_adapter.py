from __future__ import annotations

import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

import httpx

from fip.ingestion.onderwijsinspectie.adapter import OnderwijsInspectieSource


def test_inspection_source_parses_local_ods() -> None:
    temp_dir = _temp_dir()
    source_file = temp_dir / "inspection.ods"
    _write_simple_ods(
        source_file,
        [
            ("Brin", "Vestnr", "Oordeel", "Standaard"),
            ("01AB", "12", "voldoende", "OP0"),
        ],
    )

    source = OnderwijsInspectieSource(source_ref=str(source_file), run_id="run-001")
    records = list(source.iter_records())

    assert len(records) == 1
    assert records[0].source_name == "onderwijsinspectie"
    assert records[0].entity_name == "inspection.school_quality"
    assert records[0].natural_key == "brin=01AB|vestnr=12|oordeel=voldoende|standaard=OP0"
    assert records[0].payload["brin"] == "01AB"
    assert records[0].payload["vestnr"] == "12"
    assert records[0].payload["oordeel"] == "voldoende"
    assert records[0].payload["standaard"] == "OP0"


def test_inspection_source_resolves_page_download(monkeypatch) -> None:
    page_url = "https://www.onderwijsinspectie.nl/trends-en-ontwikkelingen/onderwijsdata/oordelen/oordelen-2025-2026"
    download_url = "https://www.onderwijsinspectie.nl/binaries/onderwijsinspectie/documenten/data-bestanden/2026/02/02/oordelen-1-februari-2026.ods"
    temp_dir = _temp_dir()
    source_file = temp_dir / "inspection.ods"
    _write_simple_ods(
        source_file,
        [
            ("Brin", "Vestnr", "Oordeel", "Standaard"),
            ("01CD", "7", "onvoldoende", "OP1"),
        ],
    )
    ods_bytes = source_file.read_bytes()
    download_href = (
        "/binaries/onderwijsinspectie/documenten/data-bestanden/2026/02/02/"
        "oordelen-1-februari-2026.ods"
    )
    besturen_href = (
        "/binaries/onderwijsinspectie/documenten/data-bestanden/2026/02/02/"
        "oordelen-besturen-1-februari-2026.ods"
    )
    html = (
        "<html>"
        "<body>"
        f'<a href="{download_href}">'
        "Download: Oordelen primair, speciaal en voortgezet onderwijs 1 februari 2026"
        "</a>"
        f'<a href="{besturen_href}">'
        "Download: Oordelen besturen 1 februari 2026"
        "</a>"
        "</body>"
        "</html>"
    )

    def fake_get(url: str, timeout: float, follow_redirects: bool) -> httpx.Response:
        request = httpx.Request("GET", url)
        if url == page_url:
            return httpx.Response(
                200,
                request=request,
                content=html.encode("utf-8"),
                headers={"content-type": "text/html; charset=utf-8"},
            )
        if url == download_url:
            return httpx.Response(
                200,
                request=request,
                content=ods_bytes,
                headers={"content-type": "application/vnd.oasis.opendocument.spreadsheet"},
            )
        raise AssertionError(f"Unexpected URL requested: {url}")

    monkeypatch.setattr("httpx.get", fake_get)

    source = OnderwijsInspectieSource(source_ref=page_url, run_id="run-002")
    records = list(source.iter_records())

    assert len(records) == 1
    assert records[0].natural_key == "brin=01CD|vestnr=7|oordeel=onvoldoende|standaard=OP1"
    assert records[0].payload["brin"] == "01CD"
    assert records[0].payload["oordeel"] == "onvoldoende"
    assert records[0].payload["standaard"] == "OP1"


def test_inspection_source_healthcheck_accepts_local_ods() -> None:
    source_file = _temp_dir() / "inspection.ods"
    _write_simple_ods(
        source_file,
        [
            ("Brin", "Vestnr"),
            ("01EF", "3"),
        ],
    )

    source = OnderwijsInspectieSource(source_ref=str(source_file), run_id="run-003")

    assert source.healthcheck() is True


def _write_simple_ods(path: Path, rows: list[tuple[str, ...]]) -> None:
    content = _build_ods_content(rows)
    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("content.xml", content)


def _temp_dir() -> Path:
    temp_dir = Path(__file__).resolve().parent / ".tmp_onderwijsinspectie"
    temp_dir.mkdir(exist_ok=True)
    return temp_dir


def _build_ods_content(rows: list[tuple[str, ...]]) -> str:
    header_rows = []
    for row in rows:
        cells = "".join(
            (
                '<table:table-cell office:value-type="string">'
                f"<text:p>{escape(cell)}</text:p>"
                "</table:table-cell>"
            )
            for cell in row
        )
        header_rows.append(f"<table:table-row>{cells}</table:table-row>")

    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<office:document-content "
        'xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" '
        'xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" '
        'xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0">'
        "<office:body><office:spreadsheet>"
        '<table:table table:name="Sheet1">' + "".join(header_rows) + "</table:table>"
        "</office:spreadsheet></office:body></office:document-content>"
    )
