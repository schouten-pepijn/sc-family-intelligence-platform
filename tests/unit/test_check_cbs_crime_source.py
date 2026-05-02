from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "check_cbs_crime_source.py"
SPEC = importlib.util.spec_from_file_location("check_cbs_crime_source", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:
    raise ImportError(f"Could not load script module from {SCRIPT_PATH}")

check_cbs_crime_source = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = check_cbs_crime_source
SPEC.loader.exec_module(check_cbs_crime_source)
build_lookup_tree = check_cbs_crime_source.build_lookup_tree


def test_build_lookup_tree_orders_roots_and_children() -> None:
    rows = [
        {"Id": "A", "Title": "Root"},
        {"Id": "B", "Title": "Beta", "ParentId": "A"},
        {"Id": "C", "Title": "Alpha", "ParentId": "A"},
        {"Identifier": "D", "Description": "Detached"},
    ]

    nodes, roots, children = build_lookup_tree(rows)

    assert roots == ["D", "A"]
    assert nodes["A"].label == "Root"
    assert nodes["B"].parent_code == "A"
    assert children["A"] == ["C", "B"]
