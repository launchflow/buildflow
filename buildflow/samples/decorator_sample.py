"""Sample usage of buildflow.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python decorator_sample.py
"""

import buildflow
from buildflow import Flow

# TODO(developer): add a pub/sub info.
# subscription format: 'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}'
_INPUT_SUBSCRIPTION = ''
# topic format: 'projects/{PROJECT_ID}/topic/{SUBSCRIPTION_ID}'
_OUTPUT_TOPIC = ''

flow = Flow()


@flow.processor(source=buildflow.PubSub(subscription=_INPUT_SUBSCRIPTION),
                sink=buildflow.PubSub(topic=_OUTPUT_TOPIC))
def process(taxi_info):
    print(taxi_info)
    return taxi_info


flow.run().output()
