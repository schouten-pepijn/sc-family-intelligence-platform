from fip.lakehouse.bronze.writer import IcebergSink


class BAGIcebergSinkFactory:
    ENTITY_TABLE_NAMES = {
        "bag.adres": "bag_adressen",
        "bag.adressen": "bag_adressen",
        "bag.pand": "bag_pand",
        "bag.verblijfsobject": "bag_verblijfsobject",
    }

    def __init__(self, namespace: str = "bronze") -> None:
        self.namespace = namespace

    def for_entity(self, entity_name: str) -> IcebergSink:
        table_name = self.ENTITY_TABLE_NAMES.get(entity_name)
        if not table_name:
            raise ValueError(
                f"Unknown entity name '{entity_name}'. No mapping to table name found."
            )

        return IcebergSink(table_ident=f"{self.namespace}.{table_name}")
