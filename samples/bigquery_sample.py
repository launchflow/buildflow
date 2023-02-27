"""Sample usage of buildflow reading and writing to BigQuery.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python bigquery_sample.py
"""

import logging
import buildflow as flow
import ray

# TODO(developer: Fill in the table info.
_INPUT_TABLE_ID = 'daring-runway-374503.taxi_ride_benchmark.buildflow'
_OUTPUT_TABLE_ID = 'daring-runway-374503.taxi_ride_benchmark.output_ray'


# This will read in the entire table and create a ray Dataset.
# Ray Datasets documentation: https://docs.ray.io/en/master/data/dataset.html
@flow.processor(input_ref=flow.BigQuery(table_id=_INPUT_TABLE_ID),
                output_ref=flow.BigQuery(table_id=_OUTPUT_TABLE_ID))
def process_table(dataset: ray.data.Dataset):
    # TODO: process the dataset.
    return dataset


# NOTE: You can also pass queries to the BigQuery ref.
# @flow.processor(
#     input_ref=flow.BigQuery(query=f'SELECT * FROM `{_INPUT_TABLE_ID}`'),
#     output_ref=flow.BigQuery(table_id=_OUTPUT_TABLE_ID))
# def process_query_result(dataset: ray.data.Dataset):
#     print('DATASET: ', dataset)
#     return dataset

logging.basicConfig(level=logging.INFO)
# NOTE: You can increase the number of replicas to load the dataset faster.
output = flow.run(num_replicas=1)

# NOTE: You can (optionally) do something with the processed output.
