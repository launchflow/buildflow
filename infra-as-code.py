from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.orm import Session

from buildflow import Node, Depends

# buildflow.resources.clients exposes all of the ClientResource types (for Depends API)
from buildflow.resources.clients import PostgresClientPool

# buildflow.resources.io exposes all of the IOResource types (for Provider API)
from buildflow.resources.io import BigQueryTable, GCPPubSubSubscription


@dataclass
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


# Create a new Node with all the defaults set
app = Node()

# Define the source and sink
pubsub_source = GCPPubSubSubscription(
    topic_id="projects/pubsub-public-data/topics/taxirides-realtime",
    project_id="daring-runway-374503",
    subscription_name="taxiride-sub",
)
bigquery_sink = BigQueryTable(
    table_id="daring-runway-374503.taxi_ride_benchmark.buildflow_temp",
    include_dataset=False,
    destroy_protection=False,
)
# Define the postgres client pool
db_pool = PostgresClientPool(
    host="localhost",
    port=5432,
    user="postgres",
    password="postgres",
    database="postgres",
    max_size=10,
)


# using imperative api DOES NOT REQUIRE all schema types to be provided.
@app.processor(source=pubsub_source, sink=bigquery_sink)
def my_processor(pubsub_message, db: Session = Depends(db_pool)):
    return pubsub_message


if __name__ == "__main__":
    app.run(
        disable_usage_stats=True,
        apply_infrastructure=False,
        destroy_infrastructure=False,
        start_node_server=True,
    )
