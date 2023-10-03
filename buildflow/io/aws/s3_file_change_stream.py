import dataclasses
from typing import Iterable, List, Optional

import pulumi
import pulumi_aws

from buildflow.config.cloud_provider_config import AWSOptions
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.types.aws_types import AWSAccountID, S3BucketName, SQSQueueName
from buildflow.io.aws.pulumi.providers import aws_provider
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.aws.sqs import SQSQueue
from buildflow.io.aws.strategies.s3_file_change_stream_strategies import (
    S3FileChangeStreamSource,
)
from buildflow.io.primitive import AWSPrimtive
from buildflow.io.strategies.source import SourceStrategy
from buildflow.io.utils.clients.aws_clients import AWSClients
from buildflow.types.aws import S3ChangeStreamEventType


def _get_queue_url(
    aws_conn, queue_name: SQSQueueName, aws_account_id: Optional[AWSAccountID]
):
    if aws_account_id is None:
        response = aws_conn.get_queue_url(QueueName=queue_name)
    else:
        response = aws_conn.get_queue_url(
            QueueName=queue_name, QueueOwnerAWSAccountId=aws_account_id
        )
    return response["QueueUrl"]


def _get_queue_arn(aws_conn, queue_url: str):
    queue_attrs = aws_conn.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )
    return queue_attrs["Attributes"]["QueueArn"]


@dataclasses.dataclass
class S3FileChangeStream(AWSPrimtive):
    s3_bucket: S3Bucket
    event_types: Iterable[S3ChangeStreamEventType] = (
        S3ChangeStreamEventType.OBJECT_CREATED_ALL,
    )

    # The sqs queue is always managed by the S3FileChangeStream and
    # is setup in __post_init__ based on the bucket configuration.
    sqs_queue: SQSQueue = dataclasses.field(init=False)

    def __post_init__(self):
        self.sqs_queue = SQSQueue(
            queue_name=f"{self.s3_bucket.bucket_name}_queue",
            aws_region=self.s3_bucket.aws_region,
        )
        self.sqs_queue.enable_managed()

    def primitive_id(self):
        return f"{self.s3_bucket.bucket_name}::{self.sqs_queue.queue_name}"

    @classmethod
    def from_aws_options(
        cls, aws_options: AWSOptions, bucket_name: S3BucketName
    ) -> AWSPrimtive:
        bucket = S3Bucket.from_aws_options(aws_options, bucket_name=bucket_name)
        bucket.enable_managed()
        return cls(bucket)

    def source(self, credentials: AWSCredentials) -> SourceStrategy:
        return S3FileChangeStreamSource(
            credentials=credentials,
            sqs_source=self.sqs_queue.source(credentials),
            aws_region=self.s3_bucket.aws_region,
        )

    def pulumi_resource(
        self, credentials: AWSCredentials, opts: pulumi.ResourceOptions
    ) -> List[pulumi.Resource]:
        aws_clients = AWSClients(
            credentials=credentials, region=self.sqs_queue.aws_account_id
        )
        sqs_client = aws_clients.sqs_client()
        queue_url = _get_queue_url(
            sqs_client, self.sqs_queue.queue_name, self.sqs_queue.aws_account_id
        )
        queue_arn = _get_queue_arn(sqs_client, queue_url)
        provider_id = f"{self.s3_bucket.bucket_name}-" f"{self.sqs_queue.queue_name}"
        provider = aws_provider(
            provider_id,
            aws_account_id=None,
            aws_region=self.s3_bucket.aws_region,
        )
        opts = pulumi.ResourceOptions.merge(
            opts, pulumi.ResourceOptions(provider=provider)
        )
        queue_policy_document = pulumi_aws.iam.get_policy_document_output(
            statements=[
                pulumi_aws.iam.GetPolicyDocumentStatementArgs(
                    effect="Allow",
                    principals=[
                        pulumi_aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                            type="*",
                            identifiers=["*"],
                        )
                    ],
                    actions=["sqs:SendMessage"],
                    resources=[f"arn:aws:sqs:*:*:{self.sqs_queue.queue_name}"],
                    conditions=[
                        pulumi_aws.iam.GetPolicyDocumentStatementConditionArgs(
                            test="ArnLike",
                            variable="aws:SourceArn",
                            values=[f"arn:aws:s3:*:*:{self.s3_bucket.bucket_name}"],
                        )
                    ],
                )
            ],
            opts=pulumi.InvokeOptions(parent=opts.parent, provider=provider),
        )
        policy_id = f"{self.sqs_queue.queue_name}-policy"
        queue_policy = pulumi_aws.sqs.QueuePolicy(
            resource_name=policy_id,
            opts=opts,
            queue_url=queue_url,
            policy=queue_policy_document.json,
        )
        s3_event_types = [f"s3:{et.value}" for et in self.event_types]
        notification_id = (
            f"{self.s3_bucket.bucket_name}-" f"{self.sqs_queue.queue_name}-notification"
        )
        notification = pulumi_aws.s3.BucketNotification(
            resource_name=notification_id,
            opts=opts,
            bucket=self.s3_bucket.bucket_name,
            queues=[
                pulumi_aws.s3.BucketNotificationQueueArgs(
                    queue_arn=queue_arn,
                    events=s3_event_types,
                )
            ],
        )

        return [queue_policy, notification]
