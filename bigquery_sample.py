"""Sample for reading and writing to BigQuery."""

import time
import flowstate as flow

_QUERY = """\
SELECT
  *
FROM
  `daring-runway-374503.covid_19_search_trends_symptoms_dataset.county_28d_historical`
"""

# @flow.processor(input_ref=flow.BigQuery(query=flow.BigQuery.Query(
#     _QUERY,
#     temporary_dataset='daring-runway-374503.temporary',
# )))
# def process(bq_row):
#     return bq_row

# start = time.time()
# flow.run()
# print('TOTAL SECONDS = ', time.time() - start)

from google.cloud import bigquery
from google.cloud import bigquery_storage_v1

import logging
import uuid
import sys
import ray

bq_client = bigquery.Client()
output_table = f'daring-runway-374503.temporary.{str(uuid.uuid4())}'
query_job = bq_client.query(
    _QUERY,
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
    parent=parent, read_session=read_session)

rows_per_stream = table.num_rows / len(read_session.streams)
logging.warning(
    'Reading in %s rows per stream. Increase the number of replicas '
    'if this is to large.', rows_per_stream)

logging.warning('Starting %s streams for reading from BigQuery.',
                len(read_session.streams))


s = list(read_session.streams)[0].name

@ray.remote(num_cpus=1)
class Actor:
    
    def __init__(self, stream):
        self.stream = stream
        logging.basicConfig(level=logging.INFO)

    def test_ray(self):
        storage_client = bigquery_storage_v1.BigQueryReadClient()
        logging.info('starting stream: %s', self.stream)
        response = storage_client.read_rows(self.stream)
        row_batch = []
        bytes_sum = 0
        for row in response.rows():
            py_row = dict(
                    map(lambda item: (item[0], item[1].as_py()), row.items()))
            bytes_sum += sys.getsizeof(py_row)
            row_batch.append(py_row)
        logging.info('finished processing stream: %s', self.stream)
        return row_batch


start = time.time()
actors = [Actor.remote(s.name) for s in read_session.streams]
refs = [a.test_ray.remote() for a in actors]
print('DO NOT SUBMIT: ', ray.get([a.test_ray.remote() for a in actors]))
print('DO NOT SUBMIT: ', time.time() - start)
