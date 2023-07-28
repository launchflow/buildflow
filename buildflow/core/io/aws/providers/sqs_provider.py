from typing import List, Optional, Type

import pulumi
import pulumi_aws

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.io.aws.providers.pulumi_providers import aws_provider
from buildflow.core.io.aws.strategies.sqs_strategies import SQSSink, SQSSource
from buildflow.core.providers.provider import (
    PulumiProvider,
    SinkProvider,
    SourceProvider,
)
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.strategies.sink import SinkStrategy
from buildflow.core.strategies.source import SourceStrategy
from buildflow.core.types.aws_types import AWSAccountID, AWSRegion, SQSQueueName


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

    def pulumi_resources(
        self, type_: Optional[Type], depends_on: List[PulumiResource] = []
    ) -> List[PulumiResource]:
        queue_id_components = []
        if self.aws_region is not None:
            queue_id_components.append(self.aws_region)
        queue_id_components.append(self.queue_name)
        queue_id = "-".join(queue_id_components)
        provider = aws_provider(
            queue_id, aws_account_id=self.aws_account_id, aws_region=self.aws_region
        )
        depends = [tr.resource for tr in depends_on]
        queue_resource = pulumi_aws.sqs.Queue(
            resource_name=queue_id,
            name=self.queue_name,
            opts=pulumi.ResourceOptions(depends_on=depends, provider=provider),
        )

        pulumi.export(f"aws.sqs.name.{self.queue_name}", queue_id)
        return [
            PulumiResource(
                resource_id=self.queue_name,
                resource=queue_resource,
                exports={
                    f"aws.sqs.name.{self.queue_name}": queue_id,
                },
            )
        ]
