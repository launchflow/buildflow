"""Sample usage of buildflow reading and writing to BigQuery.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python bigquery_sample.py
"""

import logging
from buildflow import Flow
import buildflow
import ray

# TODO(developer): Fill in the table info.
_INPUT_TABLE_ID = ''
_OUTPUT_TABLE_ID = ''


flow = Flow()


# This will read in the entire table and create a ray Dataset.
# Ray Datasets documentation: https://docs.ray.io/en/master/data/dataset.html
@flow.processor(input_ref=buildflow.BigQuery(table_id=_INPUT_TABLE_ID),
                output_ref=buildflow.BigQuery(table_id=_OUTPUT_TABLE_ID))
def process_table(dataset: ray.data.Dataset):
    # TODO: process the dataset (bq table).
    return dataset


# NOTE: You can also pass queries to the BigQuery ref.
@flow.processor(
    input_ref=buildflow.BigQuery(query=f'SELECT * FROM `{_INPUT_TABLE_ID}`'),
    output_ref=buildflow.BigQuery(table_id=_OUTPUT_TABLE_ID))
def process_query_result(dataset: ray.data.Dataset):
    # TODO: process the dataset (bq query result).
    return dataset


logging.basicConfig(level=logging.INFO)
# NOTE: You can increase the number of replicas to load the dataset faster.
output = flow.run(num_replicas=1)

# NOTE: You can (optionally) do something with the processed output.
