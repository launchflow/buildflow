import asyncio
import atexit
from typing import Any, Dict, Iterable, List, TypeAlias, Union
from google.cloud.bigquery.table import TableReference
from google.cloud.bigquery_storage_v1.types import (AppendRowsRequest,
                                                    CreateWriteStreamRequest,
                                                    ProtoRows, ProtoSchema,
                                                    WriteStream)
from google.protobuf.descriptor_pb2 import DescriptorProto
# from google.protobuf.message import Message
from ray.data import Dataset as RayDataset
# TODO: Make our own proto parsing library
from xia_easy_proto import EasyProto

from buildflow.io.providers import PushProvider
from buildflow.io.providers.gcp import clients as gcp_clients

BigQueryInput: TypeAlias = Union[RayDataset, Iterable[Dict[str, Any]]]

SCHEMA_SAMPLE = {
    "ride_id": "bcf590c3-74ab-4fc2-88e2-b42575e5b9c1",
    "point_idx": 509,
    "latitude": 40.720870000000005,
    "longitude": -73.98159000000001,
    "timestamp": "2023-05-22T02:52:00.78627-04:00",
    "meter_reading": 15.530334,
    "meter_increment": 0.030511463,
    "ride_status": "enroute",
    "passenger_count": 5
}


def _build_append_rows_request(serialized_rows: Iterable[bytes],
                               proto_class: Any,
                               stream_name: str) -> AppendRowsRequest:
    """
    Create AppendRowsRequest() with messages included.
    For the first request we need to include stream name and protobuf schema
    of the message. Remaining messages might skip it.
    """
    rows = ProtoRows(serialized_rows=serialized_rows)

    request = AppendRowsRequest()
    request.write_stream = stream_name
    proto_descriptor = DescriptorProto()
    proto_class().DESCRIPTOR.CopyToProto(proto_descriptor)
    request.proto_rows = AppendRowsRequest.ProtoData(
        writer_schema=ProtoSchema(proto_descriptor=proto_descriptor),
        rows=rows,
    )

    return request


class StreamingBigQueryProvider(PushProvider):

    def __init__(self, *, billing_project_id: str, table_id: str):
        super().__init__()
        # configuration
        self.table_id = table_id
        # setup
        self.bigquery_client = gcp_clients.get_bigquery_write_async_client(
            billing_project_id)
        # initial state
        self._proto_class = None
        self._write_stream_name = None
        # schedule cleanup
        atexit.register(lambda x: asyncio.run(self.cleanup))

    async def _create_write_stream(self):
        table_ref = TableReference.from_string(self.table_id)
        write_stream = await self.bigquery_client.create_write_stream(
            CreateWriteStreamRequest(
                parent=table_ref.to_bqstorage(),
                write_stream=WriteStream(type_=WriteStream.Type.COMMITTED),
            ))
        return write_stream.name

    async def push(self, batch: List[dict]):
        # Create the write stream if it doesn't exist
        if self._write_stream_name is None:
            self._write_stream_name = await self._create_write_stream()

        if not batch:
            print("EMPTY BATCH")
            return

        # Convert the batch to a list of protobuf messages
        if self._proto_class is None:
            self._proto_class, to_write = EasyProto.serialize(
                batch, sample_data=SCHEMA_SAMPLE)
        else:
            self._proto_class, to_write = EasyProto.serialize(
                batch, message_class=self._proto_class)

        # Convert the protobuf messages to an AppendRowsRequest
        request = _build_append_rows_request(to_write, self._proto_class,
                                             self._write_stream_name)

        # Send the write request
        results = await self.bigquery_client.append_rows([request])
        async for result in results:
            pass

    async def cleanup(self):
        if self._write_stream_name is not None:
            await self.bigquery_client.finalize_write_stream(
                name=self._write_stream_name)
        await self.bigquery_client.transport.close()
