import asyncio
from dataclasses import dataclass
import logging
from typing import Any, Dict, Optional

import boto3
import ray

from buildflow.api import io
from buildflow.runtime.ray_io import base


def _get_queue_url(aws_conn, queue_name, aws_account_id):
    if aws_account_id:
        response = aws_conn.get_queue_url(
            QueueName=queue_name, QueueOwnerAWSAccountId=aws_account_id)
    else:
        response = aws_conn.get_queue_url(QueueName=queue_name)
    return response['QueueUrl']


@dataclass
class SQSSource(io.StreamingSource):
    queue_name: str
    region: str = ''
    queue_owner_aws_account_id: str = ''
    batch_size: int = 10

    _queue_url: str = ''
    # Client used for testing locally.
    # NOTE: this should only be set for tests
    _test_sqs_client: Any = None

    def __post_init__(self):
        if self.batch_size > 10:
            raise ValueError('max batch size support by sqs is 10.')

    def get_boto_client(self):
        if self._test_sqs_client is not None:
            return self._test_sqs_client
        if self.region:
            return boto3.client(service_name='sqs', region_name=self.region)
        return boto3.client(service_name='sqs')

    def get_queue_url(self):
        if not self._queue_url:
            self._queue_url = _get_queue_url(self.get_boto_client(),
                                             self.queue_name,
                                             self.queue_owner_aws_account_id)
        return self._queue_url

    def setup(self):
        conn = self.get_boto_client()
        try:
            self.get_queue_url()
        except conn.exceptions.QueueDoesNotExist:
            logging.warning('Queue does not exist attempting to create')
            try:
                conn.create_queue(QueueName=self.queue_name)
            except Exception as e:
                raise ValueError(
                    'Queue does not exist, and failed to created it.') from e

    def actor(self, ray_sinks):
        return SQSSourceActor.remote(ray_sinks, self)

    def backlog(self) -> Optional[float]:
        client = self.get_boto_client()
        queue_url = self.get_queue_url()
        queue_atts = client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=['ApproximateNumberOfMessages'])
        if 'ApproximateNumberOfMessages' in queue_atts['Attributes']:
            return int(queue_atts['Attributes']['ApproximateNumberOfMessages'])
        return 0


@ray.remote(num_cpus=SQSSource.num_cpus())
class SQSSourceActor(base.StreamingRaySource):

    def __init__(self, ray_sinks: Dict[str, base.RaySink],
                 source: SQSSource) -> None:
        super().__init__(ray_sinks)
        if source._test_sqs_client is not None:
            self.sqs_client = source._test_sqs_client
        else:
            if source.region:
                self.sqs_client = boto3.client(service_name='sqs',
                                               region_name=source.region)
            else:
                self.sqs_client = boto3.client(service_name='sqs')
        self.queue_url = _get_queue_url(self.sqs_client, source.queue_name,
                                        source.queue_owner_aws_account_id)
        # This is the max messages allowed by SQS.
        self.batch_size = source.batch_size
        self.running = True

    async def run(self):
        while self.running:
            try:
                response = self.sqs_client.receive_message(
                    QueueUrl=self.queue_url,
                    AttributeNames=['All'],
                    MaxNumberOfMessages=self.batch_size)
                num_messages = 0
                if 'Messages' in response:
                    num_messages = len(response['Messages'])
                    await self._send_batch_to_sinks_and_await(
                        response['Messages'])
                    to_delete = []
                    for message in response['Messages']:
                        to_delete.append({
                            'Id':
                            message['MessageId'],
                            'ReceiptHandle':
                            message['ReceiptHandle']
                        })
                    self.sqs_client.delete_message_batch(
                        QueueUrl=self.queue_url, Entries=to_delete)
                # TODO: we should look into abstracting the while loop in
                # base.StreamingRaySource then new sources wouldn't have to
                # worry about the shutdown / reporting metrics
                self.update_metrics(num_messages)
                # Since SQS doesn't have an async client we need to
                # sleep here to yield back the event loop. This allows us to
                # collect metrics and shut down correctly.
                # Ideally we wouldn't have to put this sleep in here though.
                # TODO: look at using promethius metrics to get metrics
                # instead.
                await asyncio.sleep(.1)
            except Exception as error:
                logging.error(
                    'Couldn\'t process messages from queue: %s. The message '
                    'will not be removed and can be retried later. error: %s',
                    self.queue_url, error)

    def shutdown(self):
        print('Shutting down SQS source')
        self.running = False
        return True
