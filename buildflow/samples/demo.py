"""Sample usage of buildflow.
steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python decorator_sample.py
"""

import buildflow
from buildflow import Flow
import time

# TODO(developer): add a pub/sub info.
# subscription format: 'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}'
_INPUT_SUBSCRIPTION = 'projects/daring-runway-374503/subscriptions/buildflow-sub'
# table format: 'project.dataset.table'
_OUTPUT_TABLE = 'daring-runway-374503.taxi_ride_benchmark.buildflow'

flow = Flow(host="http://localhost:3569")


@flow.processor(
    source=buildflow.PubSubSource(subscription=_INPUT_SUBSCRIPTION),
    sink=buildflow.BigQuerySink(table_id=_OUTPUT_TABLE))
def process(pubsub_message):
    return pubsub_message


# NOTE: You can increase the number of replicas to process the messages faster.
flow.run(streaming_options=buildflow.StreamingOptions(num_replicas=2,
                                                      autoscaling=False))
