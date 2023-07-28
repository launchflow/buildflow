import dataclasses
from typing import Optional
from buildflow.config.cloud_provider_config import AWSOptions

from buildflow.core.io.primitive import AWSPrimtive
from buildflow.core.providers.provider import (
    PulumiProvider,
    SinkProvider,
    SourceProvider,
)
from buildflow.core.types.aws_types import AWSAccountID, AWSRegion, SQSQueueName
from buildflow.core.io.aws.providers.sqs_provider import SQSQueueProvider


@dataclasses.dataclass
class SQSQueue(AWSPrimtive):
    queue_name: SQSQueueName
    aws_account_id: Optional[AWSAccountID] = None
    aws_region: Optional[AWSRegion] = None

    @classmethod
    def from_aws_options(
        cls, aws_options: AWSOptions, queue_name: SQSQueueName
    ) -> AWSPrimtive:
        region = aws_options.default_region
        return cls(queue_name=queue_name, bucket_region=region)

    def source_provider(self) -> SourceProvider:
        return SQSQueueProvider(
            queue_name=self.queue_name,
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )

    def sink_provider(self) -> SinkProvider:
        return SQSQueueProvider(
            queue_name=self.queue_name,
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )

    def pulumi_provider(self) -> PulumiProvider:
        return SQSQueueProvider(
            queue_name=self.queue_name,
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )
