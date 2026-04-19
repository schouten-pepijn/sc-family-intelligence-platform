from fip.lakehouse.bronze.writer import IcebergSink


class CBSIcebergSinkFactory:
    """Factory for creating CBS-specific Bronze Iceberg sinks."""

    ENTITY_TABLE_NAMES = {
        "Observations": "observations",
        "MeasureCodes": "measure_codes",
        "PeriodenCodes": "perioden_codes",
        "RegioSCodes": "regios_codes",
    }

    def __init__(self, namespace: str = "bronze") -> None:
        self.namespace = namespace

    def for_entity(self, entity_name: str) -> IcebergSink:
        """Create a sink for a CBS entity, validating and normalizing the name."""
        table_ident = self._table_ident_for_entity(entity_name)
        return IcebergSink(table_ident=table_ident)

    def _table_ident_for_entity(self, entity_name: str) -> str:
        # Normalize CBS entity names into bronze table identifiers.
        try:
            table_id, entity = entity_name.split(".", maxsplit=1)
        except ValueError as exc:
            raise ValueError(
                f"Invalid entity name '{entity_name}'. Expected format 'table_id.entity_name'."
            ) from exc

        normalized_entity = self.ENTITY_TABLE_NAMES.get(entity)
        if normalized_entity is None:
            raise ValueError(f"Unknown entity name '{entity}'. No mapping to table name found.")

        return f"{self.namespace}.cbs_{normalized_entity}_{table_id.lower()}"
