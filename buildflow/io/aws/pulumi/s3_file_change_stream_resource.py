from typing import Iterable

import pulumi
import pulumi_aws

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.io.aws.pulumi.providers import aws_provider
from buildflow.io.aws.pulumi.sqs_resource import SQSQueueResource
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.aws.sqs import SQSQueue
from buildflow.types.aws import S3ChangeStreamEventType


class S3FileChangeStreamResource(pulumi.ComponentResource):
    def __init__(
        self,
        s3_bucket: S3Bucket,
        sqs_queue: SQSQueue,
        event_types: Iterable[S3ChangeStreamEventType],
        # pulumi_resource options (buildflow internal concept)
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
