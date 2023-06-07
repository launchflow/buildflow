import dataclasses
from datetime import datetime
import os
from typing import Any, Dict

import buildflow
from buildflow import Node, InfraConfig, SchemaValidation

gcp_project = os.environ["GCP_PROJECT"]
bigquery_table = os.environ.get("BIGQUERY_TABLE", "taxi_rides")
subscription = os.environ.get("SUBSCRIPTION", "taxi-rides-sub")
dataset = os.environ.get("DATASET", "buildflow_walkthrough")

# Set up a subscriber for the source.
# If this subscriber does not exist yet BuildFlow will create it.
input_sub = buildflow.io.GCPPubSubSubscription(
    project_id=gcp_project,
    subscription_name=subscription,
    topic_id="projects/pubsub-public-data/topics/taxirides-realtime",
)
# Set up a BigQuery table for the sink.
# If this table does not exist yet BuildFlow will create it.
output_table = buildflow.io.BigQueryTable(
    table_id=f"{gcp_project}.{dataset}.{bigquery_table}",
    destroy_protection=False,
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


infra_config = InfraConfig(
    schema_validation=SchemaValidation.LOG_WARNING,
    require_confirmation=False,
    log_level="WARNING",
)
app = Node(infra_config=infra_config)


# Define our processor.
@app.processor(source=input_sub, sink=output_table)
def process(element: Dict[str, Any]) -> TaxiOutput:
    return TaxiOutput(**element)


if __name__ == "__main__":
    app.run()
