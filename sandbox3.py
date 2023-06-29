from dataclasses import dataclass
from datetime import datetime

from buildflow import Node
from buildflow.resources.io import GCPPubSubSubscription, BigQueryTable


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


# Create a new Flow
app = Flow()


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


# Attach a processor to the Node
@app.processor(
    source=pubsub_source,
    sink=bigquery_sink,
    num_cpus=0.5,
    num_concurrency=8,
)
def my_processor(pubsub_message: TaxiOutput) -> TaxiOutput:
    return pubsub_message


if __name__ == "__main__":
    buildflow.apply(node)
    buildflow.run(node)
    buildflow.destroy(node)

    app.run(
        disable_usage_stats=True,
        # runtime-only options
        block_runtime=True,
        debug_run=False,
        # infra-only options.
        apply_infrastructure=False,
        # Ad hoc infra is really nice for quick demos / tests
        destroy_infrastructure=False,
        # server options
        start_node_server=True,
    )
