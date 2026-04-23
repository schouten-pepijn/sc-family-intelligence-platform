import pyarrow as pa

from fip.lakehouse.silver.core.sink import SilverIcebergSink
from fip.lakehouse.silver.pdok_bag.bag_adressen import to_bag_adressen_row


class BAGAdressenSink(SilverIcebergSink):
    """Writes flattened BAG adres rows to Iceberg tables in Silver."""

    def _to_silver_row(self, row: dict[str, object]) -> dict[str, object]:
        return to_bag_adressen_row(row)

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
                pa.field("adres_identificatie", pa.string(), nullable=True),
                pa.field("postcode", pa.string(), nullable=True),
                pa.field("huisnummer", pa.int64(), nullable=True),
                pa.field("huisletter", pa.string(), nullable=True),
                pa.field("toevoeging", pa.string(), nullable=True),
                pa.field("openbare_ruimte_naam", pa.string(), nullable=True),
                pa.field("woonplaats_naam", pa.string(), nullable=True),
                pa.field("bronhouder_identificatie", pa.string(), nullable=True),
                pa.field("gemeentecode", pa.string(), nullable=True),
                pa.field("gemeentenaam", pa.string(), nullable=True),
                pa.field("geometry", pa.string(), nullable=True),
            ]
        )
