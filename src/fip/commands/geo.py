from __future__ import annotations

import typer

from fip.cli import app
from fip.commands._helpers import read_silver_rows
from fip.gold.core.service import write_rows_to_sink
from fip.gold.pdok_bag.bag_geo_region_mapping_writer import BAGGeoRegionMappingLandingWriter
from fip.ingestion.locatieserver.client import LocatieserverClient
from fip.ingestion.locatieserver.mapping import to_bag_geo_region_mapping_row


@app.command("build-bag-geo-region-mapping")
def build_bag_geo_region_mapping(
    table_name: str = typer.Option(
        "bag_verblijfsobject",
        "--table",
        help="Silver table name to enrich into the geo-region mapping.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Silver Iceberg namespace. Defaults to configured silver namespace.",
    ),
    limit: int = typer.Option(
        100,
        min=1,
        help="Maximum number of Silver rows to process.",
    ),
    rows: int = typer.Option(
        10,
        min=1,
        help="Maximum number of Locatieserver rows to request per lookup.",
    ),
) -> None:
    silver_rows = read_silver_rows(table_name=table_name, namespace=namespace)
    client = LocatieserverClient()
    sink = BAGGeoRegionMappingLandingWriter(table_name="bag_geo_region_mapping")

    mapping_rows: list[dict[str, object]] = []
    for silver_row in silver_rows[:limit]:
        query_parts = [
            silver_row.get("postcode"),
            silver_row.get("huisnummer"),
            silver_row.get("openbare_ruimte_naam"),
            silver_row.get("woonplaats_naam"),
        ]
        query = " ".join(
            part.strip() if isinstance(part, str) else str(part)
            for part in query_parts
            if part is not None and part != ""
        )
        lookup_result = client.lookup(query, rows=rows)
        mapping_rows.append(
            to_bag_geo_region_mapping_row(
                silver_row,
                lookup_result,
                bag_object_type="bag_verblijfsobject",
            )
        )

    written = write_rows_to_sink(mapping_rows, sink)
    typer.echo(f"Wrote {written} BAG geo-region mapping rows")
