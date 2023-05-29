from buildflow import Node
from buildflow.io import PubSubSubscription, BigQueryTable

# Create a new Node
app = Node()
# Define the source and sink
pubsub_source = PubSubSubscription(
    subscription_id='projects/daring-runway-374503/subscriptions/taxiride-sub')
bigquery_sink = BigQueryTable(
    table_id='daring-runway-374503.taxi_ride_benchmark.buildflow')


# Attach a processor to the Node
@app.processor(source=pubsub_source, sink=bigquery_sink)
def process(pubsub_message):
    # print('Process: ', pubsub_message)
    return pubsub_message


app.run(disable_usage_stats=True,
        disable_resource_creation=True,
        blocking=True,
        debug_run=False)
