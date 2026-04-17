class CBSODataSource:
    name = "cbs_statline"
    schema_version = "v1"

    def __init__(
        self,
        table_id: str,
        run_id: str,
    ) -> None:
        self.table_id = table_id
        self.run_id = run_id
        self.base_url = f"https://datasets.cbs.nl/odata/v1/CBS/{table_id}"
