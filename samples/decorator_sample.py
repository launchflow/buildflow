"""Sample usage of buildflow.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python decorator_sample.py
"""

import buildflow as flow

# TODO(developer): add a pub/sub subscription.
# subscription format: 'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}'
_SUBSCRIPTION = ''


@flow.processor(input_ref=flow.PubSub(subscription=_SUBSCRIPTION))
def process(taxi_info):
    print(taxi_info)
    return taxi_info


# NOTE: You can increase the number of replicas to process the messages faster.
flow.run(num_replicas=1)
