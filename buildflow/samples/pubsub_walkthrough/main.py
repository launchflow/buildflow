import dataclasses
import os
from datetime import datetime
from typing import Any, Dict

import buildflow
from buildflow.io.gcp import (
    BigQueryDataset,
    BigQueryTable,
    GCPPubSubSubscription,
    GCPPubSubTopic,
)


# Define an output type for our consumer.
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


bigquery_table = os.getenv("BIGQUERY_TABLE", "taxi_rides")
gcp_project = os.getenv("GCP_PROJECT", "buildflow-internal")

# Set up a subscriber for the source.
input_source = GCPPubSubSubscription(
    project_id=gcp_project,
    subscription_name="taxi_rides",
).options(
    topic=GCPPubSubTopic(
        project_id="pubsub-public-data", topic_name="taxirides-realtime"
    ),
)
# Set up a BigQuery table for the sink.
output_table = BigQueryTable(
    BigQueryDataset(
        project_id=gcp_project, dataset_name="buildflow_pubsub_to_bigquery_test"
    ),
    table_name=bigquery_table,
).options(destroy_protection=False, schema=TaxiOutput)


app = buildflow.Flow(flow_options=buildflow.FlowOptions(require_confirmation=False))

app.manage(input_source, output_table)


# Define our processor.
@app.consumer(source=input_source, sink=output_table)
def process(element: Dict[str, Any]) -> TaxiOutput:
    return TaxiOutput(**element)
