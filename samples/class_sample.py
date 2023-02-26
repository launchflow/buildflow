"""Sample usage of buildflow.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python class_sample.py
"""

import time
from typing import Any, Dict

import buildflow as flow

# TODO(developer): Fill in with a pub/sub subscription.
# subscription format: 'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}'
_SUBSCRIPTION = ''


class MyProcessor(flow.Processor):

    @staticmethod
    def _input():
        return flow.PubSub(subscription=_SUBSCRIPTION)

    def _setup(self):
        self.t0 = time.time()
        self.num_messages = 0

    def print_messages_per_sec(self):
        elapsed = time.time() - self.t0
        print(f'{self.num_messages / elapsed} messages / sec')
        self.t0 = time.time()
        self.num_messages = 0

    def process(self, message_data: Dict[str, Any]):
        self.num_messages += 1
        if self.num_messages >= 1_000:
            self.print_messages_per_sec()
        return message_data


# NOTE: You can increase the number of replicas to process the messages faster.
flow.run(MyProcessor, num_replicas=1)
