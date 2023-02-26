"""IO connectors for Bigquery and Ray."""

import asyncio
import logging
import time
import uuid
from typing import Any, Callable, Dict, Iterable, List, Union

import google.auth
import pyarrow as pa
import ray
from buildflow.api import resources
from buildflow.runtime.ray_io import base
from google.cloud import bigquery, bigquery_storage_v1

_DEFAULT_TEMP_DATASET = 'buildflow_temp'
# 3 days
_DEFAULT_DATASET_EXPIRATION_MS = 24 * 3 * 60 * 60 * 1000


def _get_bigquery_client(project: str = ''):
    return bigquery.Client(project=project)


def _get_bigquery_storage_client():
    return bigquery_storage_v1.BigQueryReadClient()


@ray.remote
def _load_arrow_table_from_stream(stream: str) -> pa.Table:
    storage_client = _get_bigquery_storage_client()
    response = storage_client.read_rows(stream)
    return response.to_arrow()


@ray.remote
class BigQuerySourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Dict[str, base.RaySink],
        bq_read_session_stream_ids: List[str],
    ) -> None:
        super().__init__(ray_sinks)
        self.bq_read_session_stream_ids = bq_read_session_stream_ids
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def source_args(
        cls,
        io_ref: resources.BigQuery,
        num_replicas: int,
    ):
        if io_ref.billing_project:
            project = io_ref.billing_project
        else:
            _, project = google.auth.default()
        bq_client = _get_bigquery_client(project)
        if io_ref.query:
            if io_ref.temporary_dataset:
                output_table = (
                    f'{io_ref.temporary_dataset}.{str(uuid.uuid4())}')
            else:
                logging.info(
                    'temporary dataset was not provided, attempting to create'
                    ' one.')
                dataset_name = f'{project}.{_DEFAULT_TEMP_DATASET}'
                dataset = bq_client.create_dataset(dataset=dataset_name,
                                                   exists_ok=True)
                dataset.default_table_expiration_ms = _DEFAULT_DATASET_EXPIRATION_MS  # noqa: E501
                bq_client.update_dataset(
                    dataset, fields=['default_table_expiration_ms'])
                output_table = f'{dataset_name}.{str(uuid.uuid4())}'
            query_job = bq_client.query(
                io_ref.query,
                job_config=bigquery.QueryJobConfig(destination=output_table),
            )

            while not query_job.done():
                logging.info('waiting for BigQuery query to finish.')
                time.sleep(1)
        elif io_ref.table_id:
            output_table = io_ref.table_id
        else:
            raise ValueError(
                'At least one of `query` or `table_id` must be set for reading'
                ' from BigQuery.')

        table = bq_client.get_table(output_table)
        read_session = bigquery_storage_v1.types.ReadSession(
            table=(f'projects/{table.project}/datasets/'
                   f'{table.dataset_id}/tables/{table.table_id}'),
            data_format=bigquery_storage_v1.types.DataFormat.ARROW)
        storage_client = _get_bigquery_storage_client()
        parent = f'projects/{table.project}'
        read_session = storage_client.create_read_session(
            parent=parent,
            read_session=read_session,
            max_stream_count=num_replicas)

        num_streams = len(read_session.streams)
        if num_streams != 0:
            rows_per_stream = table.num_rows / num_streams
            logging.info('Starting %s streams for reading from BigQuery.',
                         num_streams)
            logging.info('Reading in %s rows per stream.', rows_per_stream)
            if num_streams < num_replicas:
                logging.warning(
                    ('You requested %s replicas, but BigQuery recommends %s '
                     'streams. Only starting %s replicas.'), num_replicas,
                    num_streams, num_streams)
            elif num_streams == num_replicas:
                logging.warning(
                    ('Number of streams (%s) matched number of replicas. You '
                     'maybe be able to get more parallelism by increasing '
                     'replicas.'), num_streams)

        # The BigQuerySourceActor instance will fan these tasks out and combine
        # them into a single ray Dataset in the run() method.
        return [([stream.name for stream in read_session.streams], )]

    async def run(self):
        tasks = []
        for stream in self.bq_read_session_stream_ids:
            tasks.append(_load_arrow_table_from_stream.remote(stream))
        arrow_subtables = await asyncio.gather(*tasks)
        # TODO: determine if we can remove the async tag from this method.
        # NOTE: This uses ray.get, so it will block / log a warning.
        ray_dataset = ray.data.from_arrow(arrow_subtables)
        return await self._send_batch_to_sinks_and_await([ray_dataset])


@ray.remote
def insert_rows(bigquery_table_id, elements: Iterable[Dict[str, Any]]):
    bq_client = _get_bigquery_client()
    return bq_client.insert_rows(bq_client.get_table(bigquery_table_id),
                                 elements)


@ray.remote
class BigQuerySinkActor(base.RaySink):

    # TODO: should make this configure able.
    _BATCH_SIZE = 10000

    def __init__(
        self,
        remote_fn: Callable,
        bq_ref: resources.BigQuery,
    ) -> None:
        super().__init__(remote_fn)
        self.bq_table_id = bq_ref.table_id

    async def _write(
        self,
        elements: Union[Iterable[Dict[str, Any]], ray.data.Dataset],
    ):
        writes = []
        for i in range(0, len(elements), self._BATCH_SIZE):
            batch = elements[i:i + self._BATCH_SIZE]
            writes.append(insert_rows.remote(self.bq_table_id, batch))
        return await asyncio.gather(*writes)


# TODO: implement the sink using the BigQueryWriteAsyncClient with proto
# schemas. This isn't time senstive sense we get some concurrnecy using the
# insert_rows remote task, but I believe this is just causing another ray
# Worker to block on another process, and there's the network call hit, so I
# think this is defintely worth doing.

# Please dont remove the code below, its a starting point for the async sink.

# from google.cloud.bigquery_storage_v1beta2 import types
# from google.cloud.bigquery_storage_v1beta2.services import big_query_write
# from google.protobuf import descriptor_pb2

# write_client = big_query_write.BigQueryWriteAsyncClient()

# def append_rows_proto2(
#         self, elements: Iterable[Dict[str, Any]],
#         write_client: big_query_write.BigQueryWriteAsyncClient):
#     """Create a write stream, write some sample data, and commit the stream."""  # noqa

#     project_id, dataset_id, table_id = self.bq_table_id.split('.')
#     parent = write_client.table_path(project_id, dataset_id, table_id)
#     write_stream = types.WriteStream()

#     # When creating the stream, choose the type. Use the PENDING type to wait
#     # until the stream is committed before it is visible. See:
#     # https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1beta2#google.cloud.bigquery.storage.v1beta2.WriteStream.Type  # noqa
#     write_stream.type_ = types.WriteStream.Type.PENDING
#     write_stream = write_client.create_write_stream(
#         parent=parent, write_stream=write_stream)

#     # Some stream types support an unbounded number of requests. Pass a
#     # generator or other iterable to the append_rows method to continuously
#     # write rows to the stream as requests are generated. Make sure to read
#     # from the response iterator as well so that the stream continues to
#     # flow.

#     proto_rows = types.ProtoRows()

#     row = sample_data_pb2.SampleData()
#     row.row_num = 1
#     row.bool_col = True
#     row.bytes_col = b"Hello, World!"
#     row.float64_col = float("+inf")
#     row.int64_col = 123
#     row.string_col = "Howdy!"
#     proto_rows.serialized_rows.append(row.SerializeToString())

#     request = types.AppendRowsRequest()
#     request.write_stream = write_stream.name
#     proto_data = types.AppendRowsRequest.ProtoData()
#     proto_data.rows = proto_rows

#     # Generate a protocol buffer representation of your message descriptor.
#     # You must inlcude this information in the first request of an
#     # append_rows stream so that BigQuery knows how to parse the
#     # serialized_rows. proto_schema = types.ProtoSchema()
#     proto_descriptor = descriptor_pb2.DescriptorProto()
#     sample_data_pb2.SampleData.DESCRIPTOR.CopyToProto(proto_descriptor)
#     proto_schema.proto_descriptor = proto_descriptor
#     proto_data.writer_schema = proto_schema
#     request.proto_rows = proto_data

#     # Set an offset to allow resuming this stream if the connection breaks.
#     # Keep track of which requests the server has acknowledged and resume the
#     # stream at the first non-acknowledged message. If the server has already
#     # processed a message with that offset, it will return an ALREADY_EXISTS
#     # error, which can be safely ignored.
#     #
#     # The first request must always have an offset of 0.
#     request.offset = 0

#     responses = write_client.append_rows(
#         iter(requests),
#         # This header is required so that the BigQuery Storage API knows
#         # which region to route the request to.
#         metadata=(("x-goog-request-params",
#                    f"write_stream={write_stream.name}"), ),
#     )

#     # For each request sent, a message is expected in the responses iterable.
#     # This sample sends 3 requests, therefore expect exactly 3 responses.
#     counter = 0
#     for response in responses:
#         counter += 1
#         print(response)

#         if counter >= 3:
#             break

#     # A PENDING type stream must be "finalized" before being committed. No
#     # new records can be written to the stream after this method has been
#     # called. write_client.finalize_write_stream(name=write_stream.name)

#     # Commit the stream you created earlier.
#     batch_commit_write_streams_request = types.BatchCommitWriteStreamsRequest(  # noqa
#     )
#     batch_commit_write_streams_request.parent = parent
#     batch_commit_write_streams_request.write_streams = [write_stream.name]
#     write_client.batch_commit_write_streams(
#         batch_commit_write_streams_request)

#     print(f"Writes to stream: '{write_stream.name}' have been committed.")
#     return write_client
