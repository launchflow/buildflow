import dataclasses
from typing import Optional

import pulumi
import pulumi_aws

from buildflow.config.cloud_provider_config import AWSOptions
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.types.aws_types import AWSAccountID, AWSRegion, SQSQueueName
from buildflow.io.aws.pulumi.providers import aws_provider
from buildflow.io.aws.strategies.sqs_strategies import SQSSink, SQSSource
from buildflow.io.primitive import AWSPrimtive
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.strategies.source import SourceStrategy


@dataclasses.dataclass
class SQSQueue(AWSPrimtive):
    queue_name: SQSQueueName
    aws_account_id: Optional[AWSAccountID] = None
    aws_region: Optional[AWSRegion] = None

    def primitive_id(self):
        queue_id_components = []
        if self.aws_region is not None:
            queue_id_components.append(self.aws_region)
        queue_id_components.append(self.queue_name)
        return "-".join(queue_id_components)

    @classmethod
    def from_aws_options(
        cls, aws_options: AWSOptions, queue_name: SQSQueueName
    ) -> AWSPrimtive:
        region = aws_options.default_region
        return cls(queue_name=queue_name, bucket_region=region)

    def source(self, credentials: AWSCredentials) -> SourceStrategy:
        return SQSSource(
            credentials=credentials,
            queue_name=self.queue_name,
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )

    def sink(self, credentials: AWSCredentials) -> SinkStrategy:
        return SQSSink(
            credentials=credentials,
            queue_name=self.queue_name,
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )

    def pulumi_resources(
        self, credentials: AWSCredentials, opts: pulumi.ResourceOptions
    ):
        provider = aws_provider(
            self.primitive_id(),
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )
        opts = pulumi.ResourceOptions.merge(
            opts, pulumi.ResourceOptions(provider=provider)
        )
        return [
            pulumi_aws.sqs.Queue(
                resource_name=self.primitive_id(), name=self.queue_name, opts=opts
            )
        ]
