"""Sample usage of buildflow.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python class_sample.py
"""

import time
from typing import Any, Dict

from buildflow import Flow
import buildflow

# TODO(developer): Fill in with a pub/sub subscription.
# subscription format: 'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}'
_SUBSCRIPTION = ''

flow = Flow()


class MyProcessor(buildflow.Processor):

    def source(self):
        return buildflow.PubSub(subscription=_SUBSCRIPTION)

    def setup(self):
        self.t0 = time.time()
        self.num_messages = 0

    # NOTE: this isn't the most accurate metric since it does not include time
    # spent in the source / sink, but its a good proxy.
    def print_messages_per_sec(self):
        elapsed = time.time() - self.t0
        print(f'{self.num_messages / elapsed} messages / sec')
        self.t0 = time.time()
        self.num_messages = 0

    def process(self, message_data: Dict[str, Any]):
        self.num_messages += 1
        if self.num_messages >= 20_000:
            self.print_messages_per_sec()
        return message_data


flow.run(MyProcessor()).output()
