"""Sample usage of buildflow reading and writing to BigQuery.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python bigquery_sample.py
"""

import argparse
import logging
from buildflow import Flow
import buildflow
import ray
import sys

# Parser to allow run time configuration of arguments
parser = argparse.ArgumentParser()
parser.add_argument('--input_table', type=str, required=True)
parser.add_argument('--output_table', type=str, required=True)
args, _ = parser.parse_known_args(sys.argv)

flow = Flow()


@flow.processor(
    # NOTE: You can alternatly just pass the table ID to read in an entire
    # table.
    source=buildflow.BigQuery(query=f'SELECT * FROM `{args.input_table}`'),
    sink=buildflow.BigQuery(table_id=args.output_table))
def process_query_result(dataset: ray.data.Dataset):
    # TODO: process the dataset (bq query result).
    return dataset


logging.basicConfig(level=logging.INFO)
output = flow.run().output()

# NOTE: You can (optionally) do something with the processed output.
