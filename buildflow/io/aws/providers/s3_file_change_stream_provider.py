from typing import Iterable, Optional, Type

import pulumi
import pulumi_aws

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.io.aws.providers.pulumi_providers import aws_provider
from buildflow.io.aws.providers.sqs_provider import SQSQueueResource
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.aws.sqs import SQSQueue
from buildflow.io.aws.strategies.s3_file_change_stream_strategies import (
    S3FileChangeStreamSource,
)
from buildflow.io.provider import PulumiProvider, SourceProvider
from buildflow.types.aws import S3ChangeStreamEventType


class _S3FileChangeStreamResource(pulumi.ComponentResource):
    def __init__(
        self,
        s3_bucket: S3Bucket,
        sqs_queue: SQSQueue,
        event_types: Iterable[S3ChangeStreamEventType],
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
        credentials: AWSCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:aws:s3:S3FileChangeStream",
            f"buildflow-{s3_bucket.bucket_name}-{sqs_queue.queue_name}",
            None,
            opts,
        )
        sqs_resource = None
        for resource in opts.depends_on:
            if (
                isinstance(resource, SQSQueueResource)
                and resource.queue_name == sqs_queue.queue_name
                and resource.aws_region == sqs_queue.aws_region
                and resource.aws_acocunt_id == sqs_queue.aws_account_id
            ):
                sqs_resource = resource.queue_resource
        if sqs_resource is None:
            raise ValueError(
                "S3 file change resource requires SQS queue as a dependency."
            )
        provider_id = f"{s3_bucket.bucket_name}-" f"{sqs_queue.queue_name}"
        provider = aws_provider(
            provider_id,
            aws_account_id=None,
            aws_region=s3_bucket.aws_region,
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
                    resources=[f"arn:aws:sqs:*:*:{sqs_queue.queue_name}"],
                    conditions=[
                        pulumi_aws.iam.GetPolicyDocumentStatementConditionArgs(
                            test="ArnLike",
                            variable="aws:SourceArn",
                            values=[f"arn:aws:s3:*:*:{s3_bucket.bucket_name}"],
                        )
                    ],
                )
            ],
            opts=pulumi.InvokeOptions(parent=self, provider=provider),
        )
        policy_id = f"{sqs_queue.queue_name}-policy"
        self.queue_policy = pulumi_aws.sqs.QueuePolicy(
            resource_name=policy_id,
            opts=pulumi.ResourceOptions(parent=self, provider=provider),
            queue_url=sqs_resource.id,
            policy=queue_policy_document.json,
        )
        s3_event_types = [f"s3:{et.value}" for et in event_types]
        notification_id = (
            f"{s3_bucket.bucket_name}-" f"{sqs_queue.queue_name}-notification"
        )
        self.notification = pulumi_aws.s3.BucketNotification(
            resource_name=notification_id,
            opts=pulumi.ResourceOptions(parent=self, provider=provider),
            bucket=s3_bucket.bucket_name,
            queues=[
                pulumi_aws.s3.BucketNotificationQueueArgs(
                    queue_arn=sqs_resource.arn,
                    events=s3_event_types,
                )
            ],
        )

        self.register_outputs(
            {
                "aws.s3.notification": self.notification.id,
                "aws.sqs.policy": self.queue_policy.id,
            }
        )


class S3FileChangeStreamProvider(SourceProvider, PulumiProvider):
    def __init__(
        self,
        *,
        s3_bucket: S3Bucket,
        sqs_queue: SQSQueue,
        # source-only options
        # pulumi-only options
        event_types: Iterable[S3ChangeStreamEventType],
    ):
        self.s3_bucket = s3_bucket
        self.sqs_queue = sqs_queue
        # source-only options
        # self.event_types = list(event_types)
        # pulumi-only options
        self.event_types = event_types

    def source(self, credentials: AWSCredentials) -> S3FileChangeStreamSource:
        return S3FileChangeStreamSource(
            credentials=credentials,
            sqs_source=self.sqs_queue.source_provider().source(credentials=credentials),
            aws_region=self.sqs_queue.aws_region,
        )

    def pulumi_resource(
        self, type_: Optional[Type], credentials: AWSCredentials, opts
    ) -> _S3FileChangeStreamResource:
        return _S3FileChangeStreamResource(
            s3_bucket=self.s3_bucket,
            sqs_queue=self.sqs_queue,
            event_types=self.event_types,
            type_=type_,
            credentials=credentials,
            opts=opts,
        )
