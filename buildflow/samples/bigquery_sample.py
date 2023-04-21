"""Sample usage of buildflow reading and writing to BigQuery.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python bigquery_sample.py
"""

import argparse
import dataclasses
import logging
from buildflow import Flow
import buildflow
import ray
import sys
from typing import Optional

# Parser to allow run time configuration of arguments
parser = argparse.ArgumentParser()
parser.add_argument('--input_table', type=str, required=True)
parser.add_argument('--output_table', type=str, required=True)
parser.add_argument('--gcs_bucket', type=str, required=True)
args, _ = parser.parse_known_args(sys.argv)


@dataclasses.dataclass
class Output:
    count: Optional[int]


flow = Flow()


@flow.processor(
    # NOTE: You can alternatly just pass the table ID to read in an entire
    # table.
    source=buildflow.BigQuerySource(
        query=f'SELECT COUNT(*) as count FROM `{args.input_table}`',
        billing_project=args.input_table.split('.')[0]),
    sink=buildflow.BigQuerySink(table_id=args.output_table,
                                temp_gcs_bucket=args.gcs_bucket))
def process_query_result(dataset: ray.data.Dataset) -> Output:
    # TODO: process the dataset (bq query result).
    return dataset


logging.basicConfig(level=logging.INFO)
output = flow.run().output()
