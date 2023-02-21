"""Sample usage of launchflow.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python sample.py

"""

import launchflow as flow

_SUBSCRIPTION = 'projects/daring-runway-374503/subscriptions/tanke-test'


@flow.processor(input_ref=flow.PubSub(subscription=_SUBSCRIPTION))
def process(taxi_info):
    print(taxi_info)
    return taxi_info


flow.run()
