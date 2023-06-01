from typing import Any, Callable, Dict, Optional, List, Type

# from google.protobuf.message import Message

# TODO: Make our own proto parsing library

from buildflow.io.providers import PushProvider
from buildflow.io.providers.gcp.utils import clients as gcp_clients
from buildflow.io.providers.schemas import converters


class StreamingBigQueryProvider(PushProvider):
    # TODO: should make this configure able.
    _BATCH_SIZE = 10_000

    def __init__(self, *, billing_project_id: str, table_id: str):
        super().__init__()
        # configuration
        self.billing_project = billing_project_id
        self.table_id = table_id
        # setup
        self.bq_client = gcp_clients.get_bigquery_client(self.billing_project)
        # initial state
        self._proto_class = None
        self._write_stream_name = None
        # schedule cleanup

    async def push(self, batch: List[dict]):
        for i in range(0, len(batch), self._BATCH_SIZE):
            batch = batch[i : i + self._BATCH_SIZE]
            errors = self.bq_client.insert_rows_json(self.table_id, batch)
            if errors:
                raise RuntimeError(f"BigQuery streaming insert failed: {errors}")

    def push_converter(
        self, user_defined_type: Optional[Type]
    ) -> Callable[[Any], Dict[str, Any]]:
        return converters.dict_push_converter(user_defined_type)
