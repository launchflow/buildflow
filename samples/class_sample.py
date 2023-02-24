"""Sample usage of buildflow.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python class_sample.py
"""

from typing import Any, Dict

import buildflow as flow

# TODO(developer): Fill in with a pub/sub subscription.
# 'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_NAME}'
_SUBSCRIPTION = ''


class MyProcessor(flow.Processor):

    @staticmethod
    def _input():
        return flow.PubSub(subscription=_SUBSCRIPTION)

    def _setup(self):
        self.client = ...

    def process(self, message_data: Dict[str, Any]):
        print(message_data)
        return message_data


flow.run(MyProcessor)
