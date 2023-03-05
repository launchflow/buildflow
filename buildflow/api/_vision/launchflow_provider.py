# Providers implement the ProcessorAPI interface, and can perform additional
# setup steps before consuming the payload. For example, the CloudRun provider
# implements the ProcessorAPI interface, and also deploys the processor to
# CloudRun before consuming the payload.

from typing import Any, TextIO

from ray.data import Dataset

from buildflow import Flow
from buildflow.io import BigQuery, HTTPEndpoint, PubSub
from buildflow.templates import CloudRun, Cron, GCSFileEventStream

flow = Flow()


# CloudRun creates a Cloud Run endpoint to host the processor.
# Compare to the HTTP Endpoint example in process_endpoint.py.
@flow.processor(template=CloudRun(
        project='project',
        public_access=True,
        endpoint=HTTPEndpoint(),
))
def cloud_run_processor(payload: Any) -> Any:
    pass


# Cron creates a Cloud Scheduler instance to run the processor based on the
# cron scheduler.
# Compare to the BigQuery example in process_batch.py.
@flow.processor(template=Cron(
    schedule='0 0 * * *',
    source=BigQuery(table_id='project.dataset.table1'),
    sink=BigQuery(table_id='project.dataset.table2'),
))
def scheduled_batch_processor(dataset: Dataset) -> Dataset:
    pass


# GCSFileEventStream creates a PubSub source that emits a message for each file upload
# that matches the glob pattern.
# Compare to the PubSub example in process_stream.py.
@flow.processor(template=GCSFileEventStream(
    glob_pattern='gs://my-bucket/*',
    pubsub=PubSub(topic='projects/project/topics/my-topic'),
    sink=BigQuery(table_id='project.dataset.table2'),
))
def file_event_processor(file: TextIO) -> Any:
    pass
