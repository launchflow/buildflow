import dataclasses
from datetime import datetime
import os
from typing import Any, Dict

import buildflow
from buildflow import Node

gcp_project = os.environ["GCP_PROJECT"]
bigquery_table = os.environ["BIGQUERY_TABLE"]

# Set up a subscriber for the source.
# If this subscriber does not exist yet BuildFlow will create it.
input_sub = buildflow.GCPPubSubSource(
    subscription=f"projects/{gcp_project}/subscriptions/taxiride-sub",
    topic="projects/pubsub-public-data/topics/taxirides-realtime",
)
# Set up a BigQuery table for the sink.
# If this table does not exist yet BuildFlow will create it.
output_table = buildflow.BigQuerySink(
    table_id=f"{gcp_project}.buildflow_walkthrough.{bigquery_table}"
)


# Define an output type for our pipeline.
# By using a dataclass we can ensure our python type hints are validated
# against the BigQuery table's schema.
@dataclasses.dataclass
class TaxiOutput:
    ride_id: str
    point_idx: int
    latitude: float
    longitude: float
    timestamp: datetime
    meter_reading: float
    meter_increment: float
    ride_status: str
    passenger_count: int


app = Node()


# Define our processor.
@app.processor(source=input_sub, sink=output_table)
def process(element: Dict[str, Any]) -> TaxiOutput:
    return element
