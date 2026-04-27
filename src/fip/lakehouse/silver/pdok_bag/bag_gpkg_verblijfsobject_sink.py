import pyarrow as pa

from fip.lakehouse.silver.core.sink import SilverIcebergSink
from fip.lakehouse.silver.pdok_bag.bag_gpkg_verblijfsobject import (
    to_bag_gpkg_verblijfsobject_row,
)


class BAGGpkgVerblijfsobjectSink(SilverIcebergSink):
    """Writes BAG GPKG verblijfsobject rows to Iceberg tables in Silver."""

    def _to_silver_row(self, row: dict[str, object]) -> dict[str, object]:
        return to_bag_gpkg_verblijfsobject_row(row)

    def _get_arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("source_name", pa.string(), nullable=False),
                pa.field("natural_key", pa.string(), nullable=False),
                pa.field("retrieved_at", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("run_id", pa.string(), nullable=False),
                pa.field("schema_version", pa.string(), nullable=False),
                pa.field("http_status", pa.int32(), nullable=False),
                pa.field("bag_id", pa.string(), nullable=False),
                pa.field(
                    "verblijfsobject_identificatie",
                    pa.string(),
                    nullable=False,
                ),
                pa.field("hoofdadres_identificatie", pa.string(), nullable=True),
                pa.field("postcode", pa.string(), nullable=True),
                pa.field("huisnummer", pa.int64(), nullable=True),
                pa.field("huisletter", pa.string(), nullable=True),
                pa.field("toevoeging", pa.string(), nullable=True),
                pa.field("woonplaats_naam", pa.string(), nullable=True),
                pa.field("openbare_ruimte_naam", pa.string(), nullable=True),
                pa.field("gebruiksdoel", pa.string(), nullable=True),
                pa.field("oppervlakte", pa.int64(), nullable=True),
                pa.field("geometry", pa.string(), nullable=True),
            ]
        )

