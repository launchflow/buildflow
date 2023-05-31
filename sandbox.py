from buildflow import Node
from buildflow.io import GCPPubSubSubscription, BigQueryTable

from buildflow.core.runtime.config import RuntimeConfig

config = RuntimeConfig(
    # initial setup options
    num_threads_per_process=1,
    num_actors_per_core=1,
    num_available_cores=1,
    # autoscale options
    autoscale=True,
    min_replicas=1,
    max_replicas=10,
    # misc
    log_level="INFO",
)

# Create a new Node
app = Node(runtime_config=config)
# Define the source and sink
pubsub_source = GCPPubSubSubscription(
    topic_id="projects/pubsub-public-data/topics/taxirides-realtime",
    subscription_id="projects/daring-runway-374503/subscriptions/taxiride-sub",
)
bigquery_sink = BigQueryTable(
    table_id="daring-runway-374503.taxi_ride_benchmark.buildflow"
)


# Attach a processor to the Node
@app.processor(source=pubsub_source, sink=bigquery_sink)
def process(pubsub_message):
    # print('Process: ', pubsub_message)
    return pubsub_message


app.run(
    disable_usage_stats=True,
    disable_resource_creation=False,
    blocking=True,
    debug_run=False,
)
