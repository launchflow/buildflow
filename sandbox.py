from dataclasses import dataclass
from datetime import datetime

from buildflow import Node
from buildflow.io import GCPPubSubSubscription, BigQueryTable


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
# runtime_config = RuntimeConfig.IO_BOUND(autoscale=True)
# infra_config = InfraConfig(
#     schema_validation=SchemaValidation.LOG_WARNING,
#     require_confirmation=False,
#     log_level="WARNING",
# )
# app = Node(runtime_config=runtime_config, infra_config=infra_config)

# Create a new Node
app = Node()

# Define the source and sink
pubsub_source = GCPPubSubSubscription(
    topic_id="projects/pubsub-public-data/topics/taxirides-realtime",
    billing_project_id="daring-runway-374503",
)
bigquery_sink = BigQueryTable(
    table_id="daring-runway-374503.taxi_ride_benchmark.buildflow"
)


# Attach a processor to the Node
@app.processor(
    source=pubsub_source, sink=bigquery_sink, num_cpus=0.5, num_concurrent_tasks=8
)
def process(pubsub_message: TaxiOutput) -> TaxiOutput:
    # print('Process: ', pubsub_message)
    # should_fail = random.randint(0, 1)
    # if should_fail:
    #     raise ValueError("Randomly failing")
    return pubsub_message


if __name__ == "__main__":
    app.run(
        disable_usage_stats=True,
        # runtime-only options
        block_runtime=True,
        debug_run=False,
        # infra-only options.
        apply_infrastructure=True,
        # Ad hoc infra is really nice for quick demos / tests
        destroy_infrastructure=True,
    )

    # these should also work:
    # app.plan()
    # app.apply()
    # app.destroy()
