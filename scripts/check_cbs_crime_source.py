from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import islice
from typing import Any

import httpx

from fip.ingestion.cbs.adapter import CBSODataSource

CRIME_TYPE_ENTITY = "SoortMisdrijfCodes"


@dataclass(frozen=True)
class LookupNode:
    code: str
    label: str
    parent_code: str | None


def is_observation(record: Any) -> bool:
    return record.entity_name.endswith(".Observations")


def main() -> None:
    args = parse_args()
    source = CBSODataSource(table_id=args.table_id, run_id=args.run_id)

    records = list(islice(source.iter_records(), args.limit))
    if not records:
        print("No records returned.")
        return

    summaries = summarize(records)
    crime_type_rows, crime_type_error = fetch_lookup_payloads(source, CRIME_TYPE_ENTITY)
    crime_type_tree = build_lookup_tree(crime_type_rows)
    print_header(args, source, len(records), summaries)
    print_entity_summary(summaries["entity_counts"])
    print_lookup_summary(summaries)
    print_observation_summary(records)
    print_crime_type_summary(records, crime_type_tree, crime_type_error)
    print_code_summary(records, "Measure")
    print_code_summary(records, "SoortMisdrijf")
    print_code_summary(records, "RegioS")
    print_code_summary(records, "Perioden")
    print_missing_value_summary(records)
    print_sample_problem_rows(records)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect CBS crime source 83648NED for pipeline usability."
    )
    parser.add_argument(
        "--table-id",
        default="83648NED",
        help="CBS table id to inspect. Default: 83648NED.",
    )
    parser.add_argument(
        "--run-id",
        default="inspect-cbs-crime",
        help="Run identifier used by the CBS source wrapper.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50000,
        help="Maximum number of raw records to inspect. Default: 50000.",
    )
    return parser.parse_args()


def summarize(records: list[Any]) -> dict[str, Any]:
    entity_counts = Counter(record.entity_name for record in records)
    observation_rows = [record for record in records if is_observation(record)]

    return {
        "entity_counts": entity_counts,
        "observations": observation_rows,
        "measure_labels": collect_code_labels(records, "MeasureCodes"),
        "period_labels": collect_code_labels(records, "PeriodenCodes"),
        "region_labels": collect_code_labels(records, "RegioSCodes"),
    }


def fetch_lookup_payloads(
    source: CBSODataSource,
    entity_name: str,
) -> tuple[list[dict[str, Any]], str | None]:
    payloads: list[dict[str, Any]] = []
    url = f"{source.base_url}/{entity_name}"

    try:
        while url:
            data = source._get(url)
            payloads.extend(row for row in data.get("value", []) if isinstance(row, dict))
            url = data.get("@odata.nextLink")
    except httpx.HTTPError as exc:
        return [], str(exc)

    return payloads, None


def collect_code_labels(records: list[Any], entity_suffix: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    for record in records:
        if not record.entity_name.endswith(entity_suffix):
            continue

        code = (
            record.payload.get("Id")
            or record.payload.get("Key")
            or record.payload.get("Identifier")
            or record.natural_key
        )
        label = (
            record.payload.get("Title")
            or record.payload.get("Description")
            or record.payload.get("Identifier")
            or str(code)
        )
        labels[str(code)] = str(label)
    return labels


def extract_lookup_code(row: dict[str, Any]) -> str | None:
    for field in ("Id", "Key", "Identifier"):
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    return None


def extract_lookup_label(row: dict[str, Any], default_code: str) -> str:
    for field in ("Title", "Description", "Identifier"):
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    return default_code


def extract_parent_code(row: dict[str, Any]) -> str | None:
    for field in ("ParentId", "ParentKey", "ParentIdentifier", "Parent"):
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    return None


def build_lookup_tree(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, LookupNode], list[str], dict[str, list[str]]]:
    nodes: dict[str, LookupNode] = {}
    children: dict[str, list[str]] = defaultdict(list)

    for row in rows:
        code = extract_lookup_code(row)
        if code is None:
            continue

        node = LookupNode(
            code=code,
            label=extract_lookup_label(row, code),
            parent_code=extract_parent_code(row),
        )
        nodes[code] = node

    for node in nodes.values():
        if node.parent_code and node.parent_code in nodes and node.parent_code != node.code:
            children[node.parent_code].append(node.code)

    roots = [
        code
        for code, node in nodes.items()
        if not node.parent_code or node.parent_code not in nodes
    ]
    roots.sort(key=lambda code: (nodes[code].label.lower(), code))

    for _parent_code, child_codes in children.items():
        child_codes.sort(key=lambda code: (nodes[code].label.lower(), code))

    return nodes, roots, children


def print_header(
    args: argparse.Namespace,
    source: CBSODataSource,
    total_rows: int,
    summaries: dict[str, Any],
) -> None:
    observations = summaries["observations"]
    print(f"table_id: {source.table_id}")
    print(f"source_name: {source.name}")
    print(f"run_id: {args.run_id}")
    print(f"rows_sampled: {total_rows}")
    print(f"observation_rows: {len(observations)}")
    print(f"base_url: {source.base_url}")
    print()


def print_entity_summary(entity_counts: Counter[str]) -> None:
    print("Entity counts")
    for entity, count in entity_counts.most_common():
        print(f"  {entity}: {count}")
    print()


def print_lookup_summary(summaries: dict[str, Any]) -> None:
    measure_labels = summaries["measure_labels"]
    period_labels = summaries["period_labels"]
    region_labels = summaries["region_labels"]

    print("Lookup entities")
    print(f"  measure_labels: {len(measure_labels)}")
    print(f"  period_labels: {len(period_labels)}")
    print(f"  region_labels: {len(region_labels)}")
    if measure_labels:
        sample_measure = next(iter(measure_labels.items()))
        print(f"  sample_measure: {sample_measure[0]} -> {sample_measure[1]}")
    print()


def print_crime_type_summary(
    records: list[Any],
    crime_type_tree: tuple[dict[str, LookupNode], list[str], dict[str, list[str]]],
    lookup_error: str | None,
) -> None:
    observations = [record for record in records if is_observation(record)]
    counts = Counter(
        record.payload.get("SoortMisdrijf")
        for record in observations
        if record.payload.get("SoortMisdrijf")
    )
    nodes, roots, children = crime_type_tree

    print("Crime type translation")
    print(f"  lookup_rows: {len(nodes)}")
    print(f"  distinct_observed_codes: {len(counts)}")
    if lookup_error:
        print(f"  lookup_warning: {lookup_error}")
    if not nodes:
        print("  lookup_labels: unavailable")
        print()
        return

    print("  observed_codes:")
    for code, count in counts.most_common(20):
        node = nodes.get(code)
        label = node.label if node is not None else "(no label found)"
        print(f"    {code}: {count} -> {label}")

    print("  hierarchy:")
    print_lookup_tree(nodes, roots, children)
    print()


def print_observation_summary(records: list[Any]) -> None:
    observations = [record for record in records if is_observation(record)]
    regions = Counter(
        record.payload.get("RegioS") for record in observations if record.payload.get("RegioS")
    )
    periods = Counter(
        record.payload.get("Perioden") for record in observations if record.payload.get("Perioden")
    )
    measures = Counter(
        record.payload.get("Measure") for record in observations if record.payload.get("Measure")
    )
    crime_types = Counter(
        record.payload.get("SoortMisdrijf")
        for record in observations
        if record.payload.get("SoortMisdrijf")
    )
    value_attrs = Counter(record.payload.get("ValueAttribute") or "None" for record in observations)
    value_present = sum(
        1 for record in observations if record.payload.get("Value") not in (None, "")
    )

    print("Observation summary")
    print(f"  rows: {len(observations)}")
    print(f"  rows_with_value: {value_present}")
    print(f"  distinct_regions: {len(regions)}")
    print(f"  distinct_periods: {len(periods)}")
    print(f"  distinct_measures: {len(measures)}")
    print(f"  distinct_crime_types: {len(crime_types)}")
    print("  value_attributes:")
    for value_attr, count in value_attrs.most_common():
        print(f"    {value_attr}: {count}")
    print()


def print_lookup_tree(
    nodes: dict[str, LookupNode],
    roots: list[str],
    children: dict[str, list[str]],
    indent: str = "    ",
    level: int = 0,
    root_codes: list[str] | None = None,
) -> None:
    if root_codes is None:
        root_codes = roots

    for code in root_codes:
        node = nodes[code]
        print(f"{indent * level}{code}: {node.label}")
        child_codes = children.get(code, [])
        if child_codes:
            print_lookup_tree(
                nodes,
                roots,
                children,
                indent=indent,
                level=level + 1,
                root_codes=child_codes,
            )


def print_code_summary(records: list[Any], field: str) -> None:
    observations = [record for record in records if is_observation(record)]
    counts = Counter(
        record.payload.get(field) for record in observations if record.payload.get(field)
    )
    if not counts:
        return

    print(f"{field} summary")
    for code, count in counts.most_common(20):
        print(f"  {code}: {count}")
    print()


def print_missing_value_summary(records: list[Any]) -> None:
    observations = [record for record in records if is_observation(record)]
    impossible = [
        record for record in observations if record.payload.get("ValueAttribute") == "Impossible"
    ]
    missing = [record for record in observations if record.payload.get("Value") in (None, "")]

    print("Missing and impossible values")
    print(f"  impossible_rows: {len(impossible)}")
    print(f"  missing_value_rows: {len(missing)}")
    print()


def print_sample_problem_rows(records: list[Any]) -> None:
    observations = [record for record in records if is_observation(record)]
    problem_rows = [
        record
        for record in observations
        if record.payload.get("ValueAttribute") == "Impossible"
        or record.payload.get("Value") in (None, "")
    ]

    print("Sample problematic rows")
    for record in problem_rows[:10]:
        payload = record.payload
        print(
            "  "
            f"Id={payload.get('Id')} "
            f"RegioS={payload.get('RegioS')} "
            f"Perioden={payload.get('Perioden')} "
            f"Measure={payload.get('Measure')} "
            f"SoortMisdrijf={payload.get('SoortMisdrijf')} "
            f"ValueAttribute={payload.get('ValueAttribute')} "
            f"Value={payload.get('Value')}"
        )

    if not problem_rows:
        print("  none")


if __name__ == "__main__":
    main()
