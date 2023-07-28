from typing import Iterable, List, Optional, Type

import pulumi
import pulumi_aws

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.io.aws.providers.pulumi_providers import aws_provider
from buildflow.core.io.aws.providers.s3_provider import S3BucketProvider
from buildflow.core.io.aws.providers.sqs_provider import SQSQueueProvider
from buildflow.core.io.aws.strategies.s3_file_change_stream_strategies import (
    S3FileChangeStreamSource,
)
from buildflow.core.providers.provider import PulumiProvider, SourceProvider
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.types.aws_types import S3ChangeStreamEventType


class S3FileChangeStreamProvider(SourceProvider, PulumiProvider):
    def __init__(
        self,
        *,
        # Only used for pulumi resources
        s3_bucket_provider: Optional[S3BucketProvider],
        sqs_queue_provider: SQSQueueProvider,
        # source-only options
        # pulumi-only options
        bucket_managed: bool = False,
        event_types: Iterable[S3ChangeStreamEventType],
    ):
        self.s3_bucket_provider = s3_bucket_provider
        self.sqs_queue_provider = sqs_queue_provider
        # source-only options
        # self.event_types = list(event_types)
        # pulumi-only options
        self.bucket_managed = bucket_managed
        self.event_types = event_types

    def source(self, credentials: AWSCredentials) -> S3FileChangeStreamSource:
        return S3FileChangeStreamSource(
            credentials=credentials,
            sqs_source=self.sqs_queue_provider.source(credentials=credentials),
            aws_region=self.sqs_queue_provider.aws_region,
        )

    def pulumi_resources(
        self, type_: Optional[Type], depends_on: List[PulumiResource] = []
    ) -> List[PulumiResource]:
        s3_resources = []
        sqs_resources = []
        if self.bucket_managed:
            s3_resources = self.s3_bucket_provider.pulumi_resources(
                type_=type_, depends_on=depends_on
            )
        sqs_resources = self.sqs_queue_provider.pulumi_resources(
            type_=type_, depends_on=depends_on
        )
        provider_id = (
            f"{self.s3_bucket_provider.bucket_name}-"
            f"{self.sqs_queue_provider.queue_name}"
        )
        provider = aws_provider(
            provider_id,
            aws_account_id=None,
            aws_region=self.s3_bucket_provider.aws_region,
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
                    resources=[f"arn:aws:sqs:*:*:{self.sqs_queue_provider.queue_name}"],
                    conditions=[
                        pulumi_aws.iam.GetPolicyDocumentStatementConditionArgs(
                            test="ArnLike",
                            variable="aws:SourceArn",
                            values=[
                                f"arn:aws:s3:*:*:{self.s3_bucket_provider.bucket_name}"
                            ],
                        )
                    ],
                )
            ],
            opts=pulumi.InvokeOptions(provider=provider),
        )
        s3_depends = [tr.resource for tr in s3_resources]
        sqs_depends = [tr.resource for tr in sqs_resources]
        depends = [tr.resource for tr in depends_on] + s3_depends + sqs_depends
        policy_id = f"{self.sqs_queue_provider.queue_name}-policy"
        queue_policy = pulumi_aws.sqs.QueuePolicy(
            resource_name=f"{self.sqs_queue_provider.queue_name}-policy",
            opts=pulumi.ResourceOptions(depends_on=depends, provider=provider),
            queue_url=sqs_resources[0].resource_id,
            policy=queue_policy_document.json,
        )
        policy_resource = PulumiResource(
            resource_id=policy_id,
            resource=queue_policy,
            exports={},
        )
        s3_event_types = [f"s3:{et.value}" for et in self.event_types]
        notification_id = (
            f"{self.s3_bucket_provider.bucket_name}-"
            f"{self.sqs_queue_provider.queue_name}-notification"
        )
        bucket_notification = pulumi_aws.s3.BucketNotification(
            resource_name=notification_id,
            opts=pulumi.ResourceOptions(
                depends_on=depends + [queue_policy], provider=provider
            ),
            bucket=self.s3_bucket_provider.bucket_name,
            queues=[
                pulumi_aws.s3.BucketNotificationQueueArgs(
                    queue_arn=sqs_resources[0].resource.arn,
                    events=s3_event_types,
                )
            ],
        )
        notification_resource = PulumiResource(
            resource_id=notification_id,
            resource=bucket_notification,
            exports={},
        )
        return s3_resources + sqs_resources + [policy_resource, notification_resource]
