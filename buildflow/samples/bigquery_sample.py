"""Sample usage of buildflow reading and writing to BigQuery.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python bigquery_sample.py
"""

import dataclasses
import os

from buildflow import ComputeNode
import buildflow
import ray
from typing import Optional


@dataclasses.dataclass
class Output:
    count: Optional[int]


app = ComputeNode()

input_table = os.environ["INPUT_TABLE"]
output_table = os.environ["OUTPUT_TABLE"]
gcs_bucket = os.environ["GCS_BUCKET"]


@app.processor(
    # NOTE: You can alternatly just pass the table ID to read in an entire
    # table.
    source=buildflow.BigQuerySource(
        query=f"SELECT COUNT(*) as count FROM `{input_table}`",
        billing_project=input_table.split(".")[0],
    ),
    sink=buildflow.BigQuerySink(table_id=output_table, temp_gcs_bucket=gcs_bucket),
)
def process_query_result(dataset: ray.data.Dataset) -> Output:
    # TODO: process the dataset (bq query result).
    return dataset


if __name__ == "__main__":
    app.run()
