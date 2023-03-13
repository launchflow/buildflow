"""Sample usage of buildflow.

steps to run:
    1. pip install .
    2. gcloud auth application-default login
    3. python decorator_sample.py
"""

import buildflow
from buildflow import Flow

# TODO(developer): add bucket info.
BUCKET_NAME = 'tanke_sample_bucket'
PROJECT_ID = 'daring-runway-374503'

flow = Flow()


@flow.processor(source=buildflow.GCSFileEventStream(bucket_name=BUCKET_NAME,
                                                    project_id=PROJECT_ID))
def my_processor(pubsub_message):
    event, blob = pubsub_message
    print(f'Event: {event}')
    print(f'Blob: {blob}')
    return None


# Run on any number of machines
flow.run(num_replicas=1)
