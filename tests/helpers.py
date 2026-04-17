import json
from pathlib import Path


def load_json_fixture(path: str) -> dict:
    fixture_path = Path("tests/fixtures") / path
    return json.loads(fixture_path.read_text(encoding="utf-8"))
