from typing import Optional, Type

import pulumi
import pulumi_aws

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.types.aws_types import AWSAccountID, AWSRegion, SQSQueueName
from buildflow.io.aws.providers.pulumi_providers import aws_provider
from buildflow.io.aws.providers.utils import arn_to_cloud_console_url
from buildflow.io.aws.strategies.sqs_strategies import SQSSink, SQSSource
from buildflow.io.provider import PulumiProvider, SinkProvider, SourceProvider
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.strategies.source import SourceStrategy


class SQSQueueResource(pulumi.ComponentResource):
    def __init__(
        self,
        queue_name: SQSQueueName,
        aws_region: Optional[AWSRegion],
        aws_account_id: Optional[AWSAccountID],
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
        credentials: AWSCredentials,
        opts: pulumi.ResourceOptions,
    ):
        self.queue_name = queue_name
        self.aws_region = aws_region
        self.aws_acocunt_id = aws_account_id
        queue_id_components = []
        if aws_region is not None:
            queue_id_components.append(aws_region)
        queue_id_components.append(queue_name)
        queue_id = "-".join(queue_id_components)

        super().__init__(
            "buildflow:aws:queue:SQS",
            f"buildflow-{queue_id}",
            None,
            opts,
        )

        outputs = {}
        provider = aws_provider(
            queue_id, aws_account_id=aws_account_id, aws_region=aws_region
        )
        self.queue_resource = pulumi_aws.sqs.Queue(
            resource_name=queue_id,
            name=queue_name,
            opts=pulumi.ResourceOptions(parent=self, provider=provider),
        )

        outputs["aws.queue.sqs"] = self.queue_resource.id
        outputs["buildflow.cloud_console.url"] = pulumi.Output.all(
            self.queue_resource.arn
        ).apply(arn_to_cloud_console_url)

        self.register_outputs(outputs)


class SQSQueueProvider(PulumiProvider, SinkProvider, SourceProvider):
    def __init__(
        self,
        queue_name: SQSQueueName,
        aws_account_id: Optional[AWSAccountID],
        aws_region: Optional[AWSRegion],
    ) -> None:
        self.queue_name = queue_name
        self.aws_account_id = aws_account_id
        self.aws_region = aws_region

    def sink(self, credentials: AWSCredentials) -> SinkStrategy:
        return SQSSink(
            credentials=credentials,
            queue_name=self.queue_name,
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )

    def source(self, credentials: AWSCredentials) -> SourceStrategy:
        return SQSSource(
            credentials=credentials,
            queue_name=self.queue_name,
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: AWSCredentials,
        opts: pulumi.ResourceOptions,
    ):
        return SQSQueueResource(
            queue_name=self.queue_name,
            aws_region=self.aws_region,
            aws_account_id=self.aws_account_id,
            type_=type_,
            credentials=credentials,
            opts=opts,
        )
