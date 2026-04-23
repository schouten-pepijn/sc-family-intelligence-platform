from datetime import datetime, timezone

from fip.gold.pdok_bag.bag_adressen_writer import BAGAdressenLandingWriter
from fip.gold.pdok_bag.bag_pand_writer import BAGPandLandingWriter
from fip.gold.pdok_bag.bag_verblijfsobject_writer import BAGVerblijfsobjectLandingWriter


def make_bag_verblijfsobject_row(natural_key: str, bag_id: str) -> dict[str, object]:
    return {
        "source_name": "bag_pdok",
        "natural_key": natural_key,
        "retrieved_at": datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "bag_id": bag_id,
        "verblijfsobject_identificatie": "0000010000057469",
        "hoofdadres_identificatie": "0000200000057534",
        "postcode": "6131BE",
        "huisnummer": 32,
        "huisletter": "A",
        "toevoeging": None,
        "woonplaats_naam": "Sittard",
        "openbare_ruimte_naam": "Steenweg",
        "gebruiksdoel": "woonfunctie",
        "oppervlakte": 72,
        "geometry": '{"type": "Point"}',
    }


def make_bag_pand_row(natural_key: str, bag_id: str) -> dict[str, object]:
    return {
        "source_name": "bag_pdok",
        "natural_key": natural_key,
        "retrieved_at": datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "bag_id": bag_id,
        "pand_identificatie": "1960100000000001",
        "pand_status": "Pand in gebruik",
        "oorspronkelijk_bouwjaar": 1974,
        "geconstateerd": "N",
        "documentdatum": "2023-03-01",
        "documentnummer": "doc-123",
        "geometry": '{"type": "Polygon"}',
    }


def make_bag_adressen_row(natural_key: str, bag_id: str) -> dict[str, object]:
    return {
        "source_name": "bag_pdok",
        "natural_key": natural_key,
        "retrieved_at": datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "bag_id": bag_id,
        "adres_identificatie": "0003010000126809",
        "postcode": "9901CP",
        "huisnummer": 32,
        "huisletter": "A",
        "toevoeging": None,
        "openbare_ruimte_naam": "Steenweg",
        "woonplaats_naam": "Sittard",
        "bronhouder_identificatie": "1883",
        "gemeentecode": "GM1883",
        "gemeentenaam": "Sittard-Geleen",
        "geometry": '{"type": "Point"}',
    }


class FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> object:
        self.statements.append(str(sql))
        return object()

    def cursor(self) -> "FakeConnection":
        return self

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def executemany(self, sql: str, params_seq: list[tuple[object, ...]]) -> object:
        self.executemany_calls.append((str(sql), params_seq))
        return object()

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def test_bag_verblijfsobject_landing_writer_truncates_and_inserts_rows(
    monkeypatch,
) -> None:
    conn = FakeConnection()
    writer = BAGVerblijfsobjectLandingWriter(table_name="bag_verblijfsobject")
    rows = [
        make_bag_verblijfsobject_row("1", "bag-1"),
        make_bag_verblijfsobject_row("2", "bag-2"),
    ]

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write(rows)

    assert written == 2
    assert writer.last_written_rows[0]["bag_id"] == "bag-1"
    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.closed is True
    assert any("CREATE SCHEMA IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("CREATE TABLE IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("TRUNCATE TABLE" in statement for statement in conn.statements)
    assert len(conn.executemany_calls) == 1
    sql, params_seq = conn.executemany_calls[0]
    assert "INSERT INTO" in sql
    assert len(params_seq) == 2
    assert params_seq[0][0] == "bag_pdok"


def test_bag_verblijfsobject_landing_writer_returns_zero_for_empty_input(
    monkeypatch,
) -> None:
    conn = FakeConnection()
    writer = BAGVerblijfsobjectLandingWriter(table_name="bag_verblijfsobject")

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write([])

    assert written == 0
    assert writer.last_written_rows == []
    assert conn.statements == []
    assert conn.executemany_calls == []
    assert conn.committed is False
    assert conn.closed is False


def test_bag_pand_landing_writer_truncates_and_inserts_rows(monkeypatch) -> None:
    conn = FakeConnection()
    writer = BAGPandLandingWriter(table_name="bag_pand")
    rows = [make_bag_pand_row("1", "pand-1"), make_bag_pand_row("2", "pand-2")]

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write(rows)

    assert written == 2
    assert writer.last_written_rows[0]["bag_id"] == "pand-1"
    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.closed is True
    assert any("CREATE SCHEMA IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("CREATE TABLE IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("TRUNCATE TABLE" in statement for statement in conn.statements)
    assert len(conn.executemany_calls) == 1
    sql, params_seq = conn.executemany_calls[0]
    assert "INSERT INTO" in sql
    assert len(params_seq) == 2
    assert params_seq[0][0] == "bag_pdok"


def test_bag_pand_landing_writer_returns_zero_for_empty_input(monkeypatch) -> None:
    conn = FakeConnection()
    writer = BAGPandLandingWriter(table_name="bag_pand")

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write([])

    assert written == 0
    assert writer.last_written_rows == []
    assert conn.statements == []
    assert conn.executemany_calls == []
    assert conn.committed is False
    assert conn.closed is False


def test_bag_adressen_landing_writer_truncates_and_inserts_rows(monkeypatch) -> None:
    conn = FakeConnection()
    writer = BAGAdressenLandingWriter(table_name="bag_adressen")
    rows = [
        make_bag_adressen_row("1", "adres-1"),
        make_bag_adressen_row("2", "adres-2"),
    ]

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write(rows)

    assert written == 2
    assert writer.last_written_rows[0]["bag_id"] == "adres-1"
    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.closed is True
    assert any("CREATE SCHEMA IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("CREATE TABLE IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("TRUNCATE TABLE" in statement for statement in conn.statements)
    assert len(conn.executemany_calls) == 1
    sql, params_seq = conn.executemany_calls[0]
    assert "INSERT INTO" in sql
    assert len(params_seq) == 2
    assert params_seq[0][0] == "bag_pdok"


def test_bag_adressen_landing_writer_returns_zero_for_empty_input(monkeypatch) -> None:
    conn = FakeConnection()
    writer = BAGAdressenLandingWriter(table_name="bag_adressen")

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write([])

    assert written == 0
    assert writer.last_written_rows == []
    assert conn.statements == []
    assert conn.executemany_calls == []
    assert conn.committed is False
    assert conn.closed is False
