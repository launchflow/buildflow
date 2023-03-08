# Templates implement the ProcessorAPI interface, and can perform additional
# setup steps before consuming the payload. For example, the CloudRun template
# implements the ProcessorAPI interface, and also deploys the processor to
# CloudRun before consuming the payload.

from typing import Any, TextIO

from ray.data import Dataset

from buildflow import Flow
from buildflow.io import BigQuery, HTTPEndpoint, PubSub
from buildflow.templates import CloudRun, CloudScheduler, GCSFileEventStream

flow = Flow()


# CloudRun creates a Cloud Run endpoint to host the processor.
# Compare to the HTTP Endpoint example in process_endpoint.py.
@flow.processor(template=CloudRun(
    project='my_project',
    public_access=True,
    endpoint=HTTPEndpoint(host='localhost', port=3569),
))
def cloud_run_processor(payload: Any) -> Any:
    pass


# CloudScheduler creates a Cloud Scheduler instance to run the processor based
# on the cron schedule.
# Compare to the BigQuery example in process_batch.py.
@flow.processor(template=CloudScheduler(
    cron_schedule='0 0 * * *',
    source=BigQuery(table_id='project.dataset.table1'),
    sink=BigQuery(table_id='project.dataset.table2'),
))
def scheduled_batch_processor(dataset: Dataset) -> Dataset:
    pass


# GCSFileEventStream creates a PubSub source that emits a message for each file
# upload that matches the glob pattern.
# Compare to the PubSub example in process_stream.py.
@flow.processor(template=GCSFileEventStream(
    glob_pattern='gs://my-bucket/*',
    pubsub=PubSub(topic='projects/project/topics/my-topic'),
    sink=BigQuery(table_id='project.dataset.table2'),
))
def file_event_processor(file: TextIO) -> Any:
    pass
