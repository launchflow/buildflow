import asyncio
from typing import Any, Callable, Dict, List, Optional, Type

from buildflow.core.credentials import GCPCredentials
from buildflow.core.types.gcp_types import BigQueryTableID, BigQueryTableName
from buildflow.io.gcp.bigquery_dataset import BigQueryDataset
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.utils.clients import gcp_clients
from buildflow.io.utils.schemas import converters


class StreamingBigQueryTableSink(SinkStrategy):
    def __init__(
        self,
        *,
        credentials: GCPCredentials,
        dataset: BigQueryDataset,
        table_name: BigQueryTableName,
        batch_size: int = 10_000,
    ):
        super().__init__(
            credentials=credentials, strategy_id="streaming-bigquery-table-sink"
        )
        # configuration
        self.project_id = dataset.project_id
        self.dataset_name = dataset.dataset_name
        self.table_name = table_name
        self.batch_size = batch_size
        # setup
        clients = gcp_clients.GCPClients(
            credentials=credentials,
            quota_project_id=self.project_id,
        )
        self.bq_client = clients.get_bigquery_client()

    @property
    def table_id(self) -> BigQueryTableID:
        return f"{self.project_id}.{self.dataset_name}.{self.table_name}"

    def _insert_rows(self, rows: List[Dict[str, Any]]):
        errors = self.bq_client.insert_rows_json(self.table_id, rows)
        if errors:
            raise RuntimeError(f"BigQuery streaming insert failed: {errors}")

    async def push(self, batch: List[dict]):
        coros = []
        loop = asyncio.get_event_loop()
        for i in range(0, len(batch), self.batch_size):
            rows = batch[i : i + self.batch_size]
            coros.append(loop.run_in_executor(None, self._insert_rows, rows))
        await asyncio.gather(*coros)

    def push_converter(
        self, user_defined_type: Optional[Type]
    ) -> Callable[[Any], Dict[str, Any]]:
        return converters.json_push_converter(user_defined_type)
