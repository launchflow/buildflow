from dataclasses import dataclass
from datetime import datetime

from buildflow import Node
from buildflow.io import GCPPubSubSubscription, BigQueryTable
from buildflow.core.runtime.config import RuntimeConfig
from buildflow.core.infra.config import InfraConfig, SchemaValidation

# Fleshing options out to show all global defaults while developing

runtime_config = RuntimeConfig(
    # initial setup options
    num_threads_per_process=8,
    num_actors_per_core=2,
    num_available_cores=10,
    # autoscale options
    autoscale=True,
    min_replicas=1,
    max_replicas=20,
    # misc
    log_level="INFO",
)

infra_config = InfraConfig(
    schema_validation=SchemaValidation.LOG_WARNING,
    require_confirmation=False,
    log_level="WARNING",
)


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


# Create a new Node
app = Node(runtime_config=runtime_config, infra_config=infra_config)

# Define the source and sink
pubsub_source = GCPPubSubSubscription(
    topic_id="projects/pubsub-public-data/topics/taxirides-realtime",
    billing_project_id="daring-runway-374503",
)
bigquery_sink = BigQueryTable(
    table_id="daring-runway-374503.taxi_ride_benchmark.buildflow"
)


# Attach a processor to the Node
@app.processor(source=pubsub_source, sink=bigquery_sink)
def process(pubsub_message: TaxiOutput) -> TaxiOutput:
    # print('Process: ', pubsub_message)
    # should_fail = random.randint(0, 1)
    # if should_fail:
    #     raise ValueError("Randomly failing")
    return pubsub_message


app.run(
    disable_usage_stats=True,
    # runtime-only options
    block_runtime=True,
    debug_run=False,
    # infra-only options.
    apply_infrastructure=True,
    destroy_infrastructure=True,  # Ad hoc infra is really nice for quick demos / tests
)

# these should also work:
# app.plan()
# app.apply()
# app.destroy()
