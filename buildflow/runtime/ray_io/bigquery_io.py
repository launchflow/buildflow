"""IO connectors for Bigquery and Ray."""

import asyncio
import dataclasses
import inspect
import json
import logging
import time
from typing import Any, Callable, Dict, Iterable, List, Union

from google.api_core import exceptions
from google.cloud import bigquery, bigquery_storage_v1
import pyarrow as pa
import ray

from buildflow import utils
from buildflow.api import io
from buildflow.runtime.ray_io import base
from buildflow.runtime.ray_io.gcp import clients
from buildflow.runtime.ray_io.schemas import bigquery as bq_schemas

_DEFAULT_TEMP_DATASET = 'buildflow_temp'
_DEFAULT_TEMP_BUCKET = 'buildflow_temp'
# 3 days
_DEFAULT_DATASET_EXPIRATION_MS = 24 * 3 * 60 * 60 * 1000
_MAX_STREAM_COUNT = 1000


@dataclasses.dataclass
class BigQuerySource(io.Source):
    """Source for reading data from BigQuery."""
    # The BigQuery table to read from.
    # Should be of the format project.dataset.table
    # One and only one of table_id and query should be provided.
    table_id: str = ''
    # The query to read data from.
    # One and only one of table_id and query should be provided.
    # NOTE: You must provide `billing_project` if using a query.
    query: str = ''
    # The temporary dataset to store query results in. If unspecified we will
    # attempt to create one.
    temp_dataset: str = ''
    # The billing project to use for query usage. If unset we will use attempt
    # to user the project from `table_id`.
    # NOTE: This must be provided if you are using a query.
    billing_project: str = ''

    def __post_init__(self):
        if self.query and self.table_id:
            raise ValueError(
                'Only one of query and table_id should be provided.')
        if not self.billing_project:
            if self.query:
                raise ValueError(
                    'billing_project must be provided if using a query')
            split_table = self.table_id.split('.')
            self.billing_project = split_table[0]

    def setup(self):
        client = clients.get_bigquery_client(self.billing_project)
        if self.table_id:
            try:
                client.get_table(table=self.table_id)
            except Exception as e:
                raise ValueError(
                    f'Failed to retrieve BigQuery table: {self.table_id} '
                    'for reading. Please ensure this table exists and you '
                    'have access.') from e
        if self.query:
            try:
                client.query(self.query,
                             job_config=bigquery.QueryJobConfig(dry_run=True))
            except Exception as e:
                raise ValueError(
                    f'Failed to test BigQuery query. Failed with: {e}') from e

    def actor(self, ray_sinks):
        bq_client = clients.get_bigquery_client(self.billing_project)
        if self.query:
            if self.temp_dataset:
                output_table = (f'{self.temp_dataset}.{utils.uuid()}')
            else:
                logging.info(
                    'temporary dataset was not provided, attempting to create'
                    ' one.')
                dataset_name = (
                    f'{self.billing_project}.{_DEFAULT_TEMP_DATASET}')
                dataset = bq_client.create_dataset(dataset=dataset_name,
                                                   exists_ok=True)
                dataset.default_table_expiration_ms = _DEFAULT_DATASET_EXPIRATION_MS  # noqa: E501
                bq_client.update_dataset(
                    dataset, fields=['default_table_expiration_ms'])
                output_table = f'{dataset_name}.{utils.uuid()}'
            query_job = bq_client.query(
                self.query,
                job_config=bigquery.QueryJobConfig(destination=output_table),
            )

            while not query_job.done():
                logging.info('waiting for BigQuery query to finish.')
                time.sleep(1)
        elif self.table_id:
            output_table = self.table_id
        else:
            raise ValueError(
                'At least one of `query` or `table_id` must be set for reading'
                ' from BigQuery.')

        table = bq_client.get_table(output_table)
        read_session = bigquery_storage_v1.types.ReadSession(
            table=(f'projects/{table.project}/datasets/'
                   f'{table.dataset_id}/tables/{table.table_id}'),
            data_format=bigquery_storage_v1.types.DataFormat.ARROW)
        storage_client = clients.get_bigquery_storage_client(
            self.billing_project)
        parent = f'projects/{table.project}'
        read_session = storage_client.create_read_session(
            parent=parent,
            read_session=read_session,
            max_stream_count=_MAX_STREAM_COUNT)

        # The BigQuerySourceActor instance will fan these tasks out and combine
        # them into a single ray Dataset in the run() method.
        streams = [stream.name for stream in read_session.streams]
        return BigQuerySourceActor.remote(ray_sinks, streams,
                                          self.billing_project)


@dataclasses.dataclass
class BigQuerySink(io.Sink):
    """Sink for writing data to BigQuery."""
    # The BigQuery table to read from.
    # Should be of the format project.dataset.table
    table_id: str
    # The temporary gcs bucket uri to store temp data in. This is only used in
    # batch mode.
    # TODO: we should attempt to create this if it doesn't exist.
    temp_gcs_bucket: str = ''
    # The billing project to use for usage. If not set we will bill the project
    # that the table exists in.
    billing_project: str = ''

    def __post_init__(self):
        if not self.billing_project:
            split_table = self.table_id.split('.')
            self.billing_project = split_table[0]

    def setup(self, process_arg_spec: inspect.FullArgSpec):
        client = clients.get_bigquery_client(self.billing_project)
        schema = None
        if 'return' in process_arg_spec.annotations:
            return_type = process_arg_spec.annotations['return']
            if hasattr(return_type, '__args__'):
                # Using a composite type hint like List or Optional
                return_type = return_type.__args__[0]
            if not dataclasses.is_dataclass(return_type):
                print('Output type was not a dataclass cannot validate '
                      f'schema. Return type: {return_type}')
            else:
                schema = bq_schemas.dataclass_to_bq_schema(
                    dataclasses.fields(return_type))
                schema.sort(key=lambda sf: sf.name)
        else:
            print('No output type provided. Cannot validate BigQuery table.')
        try:
            table = client.get_table(table=self.table_id)
            bq_schema = table.schema
            bq_schema.sort(key=lambda sf: sf.name)
            if schema is not None and schema != bq_schema:
                only_in_bq = set(bq_schema) - set(schema)
                only_in_pytype = set(schema) - set(bq_schema)
                error_str = ['Output schema did not match table schema.']
                if only_in_bq:
                    error_str.append(
                        'Fields found only in BQ schema:\n'
                        f'{bq_schemas.schema_fields_to_str(only_in_bq)}'  # noqa: E501
                    )
                if only_in_pytype:
                    error_str.append(
                        'Fields found only in PyType schema:\n'
                        f'{bq_schemas.schema_fields_to_str(only_in_pytype)}'  # noqa: E501
                    )
                raise ValueError('\n'.join(error_str))
        except exceptions.NotFound:
            if schema is not None:
                print(f'creating table: {self.table_id}')
                dataset_ref = '.'.join(self.table_id.split('.')[0:2])
                client.create_dataset(dataset_ref, exists_ok=True)
                table = client.create_table(
                    bigquery.Table(self.table_id, schema))
            else:
                raise ValueError(
                    'BigQuery table does not exist and cannot create based on'
                    ' your processor output type. Please either create the '
                    'table or provde a dataclass as your output.')
        except exceptions.PermissionDenied as e:
            raise ValueError(
                f'Failed to retrieve BigQuery table: {self.table_id} '
                'for writing. Please ensure this table exists and you '
                'have access.') from e

    def actor(self, remote_fn: Callable, is_streaming: bool):
        return BigQuerySinkActor.remote(remote_fn, self, is_streaming)


@ray.remote
def _load_arrow_table_from_stream(stream: str, project: str) -> pa.Table:
    storage_client = clients.get_bigquery_storage_client(project)
    response = storage_client.read_rows(stream)
    return response.to_arrow()


@ray.remote(num_cpus=BigQuerySource.num_cpus())
class BigQuerySourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Dict[str, base.RaySink],
        bq_read_session_stream_ids: List[str],
        billing_project: str
    ) -> None:
        super().__init__(ray_sinks)
        self.bq_read_session_stream_ids = bq_read_session_stream_ids
        self.billing_project = billing_project
        logging.basicConfig(level=logging.INFO)

    async def run(self):
        if len(self.bq_read_session_stream_ids) == 1:
            stream = self.bq_read_session_stream_ids[0]
            response = clients.get_bigquery_storage_client(
                self.billing_project).read_rows(stream)
            arrow_subtables = [response.to_arrow()]
        else:
            tasks = []
            for stream in self.bq_read_session_stream_ids:
                tasks.append(
                    _load_arrow_table_from_stream.remote(
                        stream, self.billing_project))
            arrow_subtables = await asyncio.gather(*tasks)
        # TODO: determine if we can remove the async tag from this method.
        # NOTE: This uses ray.get, so it will block / log a warning.
        ray_dataset = ray.data.from_arrow(arrow_subtables)
        return await self._send_batch_to_sinks_and_await([ray_dataset])


def run_load_job_and_wait(bigquery_table_id: str, gcs_glob_uri: str,
                          source_format: bigquery.SourceFormat, project: str):
    bq_client = clients.get_bigquery_client(project)
    job_config = bigquery.LoadJobConfig(
        source_format=source_format,
        # TODO: Autodetect can be kind of awful we should set this
        # if we can based on our output schema.
        autodetect=True,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = bq_client.load_table_from_uri(gcs_glob_uri,
                                             bigquery_table_id,
                                             job_config=job_config)
    # TODO: add error handling
    load_job.result()
    return


@ray.remote(num_cpus=0.25)
def json_rows_streaming(json_rows: Iterable[Dict[str, Any]],
                        bigquery_table_id: str, project: str) -> None:
    bq_client = clients.get_bigquery_client(project)
    errors = bq_client.insert_rows_json(bigquery_table_id, json_rows)
    if errors:
        raise RuntimeError(f'BigQuery streaming insert failed: {errors}')


@ray.remote
def json_rows_load_job(json_rows: Iterable[Dict[str,
                                                Any]], bigquery_table_id: str,
                       gcs_bucket: str, project: str,
                       billing_project: str) -> str:
    storage_client = clients.get_storage_client(billing_project)
    bucket = storage_client.bucket(gcs_bucket)
    job_uuid = utils.uuid()
    json_file_contents = '\n'.join(json.dumps(row) for row in json_rows)
    batch_blob = bucket.blob(f'{job_uuid}/{job_uuid}.json')
    batch_blob.upload_from_string(json_file_contents)
    gcs_glob_uri = f'gs://{gcs_bucket}/{job_uuid}/*'
    return run_load_job_and_wait(bigquery_table_id, gcs_glob_uri,
                                 bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                 project)


@ray.remote
def ray_dataset_load_job(dataset: ray.data.Dataset, bigquery_table_id: str,
                         gcs_bucket: str, project: str) -> str:
    gcs_output_dir = f'gs://{gcs_bucket}/{utils.uuid()}'
    dataset.write_parquet(gcs_output_dir)
    gcs_glob_uri = f'{gcs_output_dir}/*'
    return run_load_job_and_wait(bigquery_table_id, gcs_glob_uri,
                                 bigquery.SourceFormat.PARQUET, project)


@ray.remote(num_cpus=BigQuerySink.num_cpus())
class BigQuerySinkActor(base.RaySink):

    # TODO: should make this configure able.
    _BATCH_SIZE = 10_000

    def __init__(
        self,
        remote_fn: Callable,
        bq_ref: BigQuerySink,
        use_streaming: bool = True,
    ) -> None:
        super().__init__(remote_fn)
        self.bq_table_id = bq_ref.table_id
        self.project = bq_ref.billing_project

        if not use_streaming and not bq_ref.temp_gcs_bucket:
            raise ValueError('`temp_gcs_bucket` must be provided for batch.')
        self.temp_gcs_bucket = bq_ref.temp_gcs_bucket

        self.use_streaming = use_streaming
        self.bq_client = clients.get_bigquery_client(self.project)

    async def _write(
        self,
        elements: Union[ray.data.Dataset, Iterable[Dict[str, Any]]],
    ):
        tasks = []
        if isinstance(elements, ray.data.Dataset):
            tasks.append(
                ray_dataset_load_job.remote(elements, self.bq_table_id,
                                            self.temp_gcs_bucket,
                                            self.project))
        else:
            for i in range(0, len(elements), self._BATCH_SIZE):
                batch = elements[i:i + self._BATCH_SIZE]
                if self.use_streaming:
                    errors = self.bq_client.insert_rows_json(
                        self.bq_table_id, batch)
                    if errors:
                        raise RuntimeError(
                            f'BigQuery streaming insert failed: {errors}')
                elif isinstance(batch[0], ray.data.Dataset):
                    for ds in batch:
                        tasks.append(
                            ray_dataset_load_job.remote(
                                ds, self.bq_table_id, self.temp_gcs_bucket,
                                self.project))
                else:
                    tasks.append(
                        json_rows_load_job.remote(batch, self.bq_table_id,
                                                  self.temp_gcs_bucket,
                                                  self.project))
        return await asyncio.gather(*tasks)


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
