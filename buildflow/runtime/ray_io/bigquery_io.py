"""IO connectors for Bigquery and Ray."""

import asyncio
import logging
import time
from typing import Any, Callable, Dict, Iterable
import uuid

import google.auth
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
import ray

from buildflow.api import resources
from buildflow.runtime.ray_io import base

_DEFAULT_TEMP_DATASET = 'buildflow_temp'
# 3 days
_DEFAULT_DATASET_EXPIRATION_MS = 24 * 3 * 60 * 60 * 1000


def _get_bigquery_client(project: str = ''):
    return bigquery.Client(project=project)


@ray.remote
class BigQuerySourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Dict[str, base.RaySink],
        stream: str,
    ) -> None:
        super().__init__(ray_sinks)
        self.stream = stream
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def source_inputs(
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
        storage_client = bigquery_storage_v1.BigQueryReadClient()
        parent = f'projects/{table.project}'
        read_session = storage_client.create_read_session(
            parent=parent,
            read_session=read_session,
            max_stream_count=num_replicas)

        num_streams = len(read_session.streams)
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
            logging.warning((
                'Number of streams (%s) matched number of replicas. You '
                'maybe be able to get more parallelism by increasing replicas.'
            ), num_streams)

        bigquery_sources = []
        for stream in read_session.streams:
            bigquery_sources.append((stream.name, ))
        return bigquery_sources

    async def run(self):
        storage_client = bigquery_storage_v1.BigQueryReadClient()
        response = storage_client.read_rows(self.stream)
        row_batch = []
        for row in response.rows():
            py_row = dict(
                map(lambda item: (item[0], item[1].as_py()), row.items()))
            row_batch.append(py_row)
        return await self.send_to_sinks(row_batch)


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

    async def insert_rows(self, elements: Iterable[Dict[str, Any]]):
        bq_client = _get_bigquery_client()
        return bq_client.insert_rows(bq_client.get_table(self.bq_table_id),
                                     elements)

    async def _write(
        self,
        elements: Iterable[Dict[str, Any]],
    ):
        writes = []
        for i in range(0, len(elements), self._BATCH_SIZE):
            writes.append(
                insert_rows.remote(self.bq_table_id,
                                   elements[i:i + self._BATCH_SIZE]))
        return await asyncio.gather(*writes)
