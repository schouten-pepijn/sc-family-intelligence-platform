from pathlib import Path

from fip.readback import duckdb as duckdb_readback
from fip.settings import Settings


def test_connect_creates_parent_directory_before_opening_database(
    monkeypatch, tmp_path
) -> None:
    target_path = tmp_path / ".duckdb" / "fip.duckdb"
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "fip.readback.duckdb.get_settings",
        lambda: Settings(duckdb_path=str(target_path)),
    )

    def fake_connect(path: str) -> object:
        captured["path"] = path
        captured["parent_exists"] = Path(path).parent.exists()
        return object()

    monkeypatch.setattr("fip.readback.duckdb.duckdb.connect", fake_connect)

    conn = duckdb_readback.connect()

    assert conn is not None
    assert captured["path"] == str(target_path)
    assert captured["parent_exists"] is True
