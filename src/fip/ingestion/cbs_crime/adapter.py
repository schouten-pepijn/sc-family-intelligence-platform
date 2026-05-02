from __future__ import annotations

from fip.ingestion.cbs.adapter import CBSODataSource


class CBSCrimeSource(CBSODataSource):
    """Temporary wrapper for CBS registered crime table 83648NED."""

    name = "cbs_crime"
    table_id = "83648NED"

    def __init__(self, run_id: str) -> None:
        super().__init__(table_id=self.table_id, run_id=run_id)
