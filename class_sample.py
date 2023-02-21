"""Sample usage of flowstate.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python sample.py

"""

import flowstate as flow

_SUBSCRIPTION = 'projects/daring-runway-374503/subscriptions/tanke-test'


class MyProcessor(flow.Processor):

    @staticmethod
    def _input():
        return flow.PubSub(subscription=_SUBSCRIPTION)

    def _setup(self):
        # this is where you would initialize any clients / shared state
        self.client = ...

    def process(self, taxi_info):
        print(taxi_info)
        return taxi_info


flow.run(MyProcessor)
