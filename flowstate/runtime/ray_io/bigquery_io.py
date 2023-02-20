"""IO connectors for Bigquery and Ray."""

import asyncio
import logging
import time
from typing import Any, Callable, Dict, Iterable, Union
import uuid

from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
import ray

from flowstate.api import resources
from flowstate.runtime.ray_io import base


def _get_bigquery_client():
    return bigquery.Client()


@ray.remote
class BigQuerySourceActor(base.RaySource):

    def __init__(
        self,
        ray_sinks: Iterable[base.RaySink],
        stream: str,
        batch_size: int
    ) -> None:
        super().__init__(ray_sinks)
        self.stream = stream
        self.batch_size = batch_size

    @classmethod
    def source_inputs(cls, io_ref: resources.BigQuery, num_replicas: int):
        bq_client = _get_bigquery_client()
        if io_ref.query is None:
            raise ValueError(
                'Please provide a query. Reading directly from a table is not '
                'currently supported.')
        output_table = f'{io_ref.query.temporary_dataset}.{str(uuid.uuid4())}'
        query_job = bq_client.query(
            io_ref.query.query,
            job_config=bigquery.QueryJobConfig(destination=output_table),
        )

        while not query_job.done():
            logging.warning('waiting for BigQuery query to finish.')
            time.sleep(1)

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

        rows_per_stream = table.num_rows / len(read_session.streams)
        logging.warning(
            'Reading in %s rows per stream. Increase the number of replicas '
            'if this is to large.', rows_per_stream)

        logging.warning('Starting %s streams for reading from BigQuery.',
                        len(read_session.streams))

        return [[s.name, io_ref.batch_size] for s in read_session.streams]

    def run(self):
        storage_client = bigquery_storage_v1.BigQueryReadClient()
        response = storage_client.read_rows(self.stream)
        refs = []
        results = []
        count = 0
        for row in response.rows():
            count += 1
            if count >= self.batch_size:
                batch_results = ray.get(refs)
                results.extend(batch_results)
                count = 0
                refs = []
            py_row = dict(
                map(lambda item: (item[0], item[1].as_py()), row.items()))
            for ray_sink in self.ray_sinks:
                refs.append(ray_sink.write.remote(dict(py_row)))
        # TODO: need to add these to results
        return ray.get(*refs)


@ray.remote
class BigQuerySinkActor(base.RaySink):

    def __init__(
        self,
        remote_fn: Callable,
        bq_ref: resources.BigQuery,
    ) -> None:
        super().__init__(remote_fn)
        self.bq_table_id = f'{bq_ref.project}.{bq_ref.dataset}.{bq_ref.table}'

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        bq_client = _get_bigquery_client()
        to_insert = element
        if isinstance(element, dict):
            to_insert = [element]
        return bq_client.insert_rows(self.bq_table_id, to_insert)
