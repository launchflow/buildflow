import dataclasses
from typing import Optional
from buildflow.config.cloud_provider_config import AWSOptions

from buildflow.core.io.primitive import AWSPrimtive
from buildflow.core.providers.provider import (
    PulumiProvider,
    SinkProvider,
    SourceProvider,
)
from buildflow.core.types.aws_types import AWSAccountID, AWSRegion, QueueName
from buildflow.core.io.aws.providers.sqs_provider import SQSProvider


@dataclasses.dataclass
class SQS(AWSPrimtive):
    queue_name: QueueName
    aws_account_id: Optional[AWSAccountID] = None
    aws_region: Optional[AWSRegion] = None

    @classmethod
    def from_aws_options(cls, aws_options: AWSOptions) -> AWSPrimtive:
        return super().from_aws_options(aws_options)

    def source_provider(self) -> SourceProvider:
        return SQSProvider(
            queue_name=self.queue_name,
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )

    def sink_provider(self) -> SinkProvider:
        return SQSProvider(
            queue_name=self.queue_name,
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )

    def pulumi_provider(self) -> PulumiProvider:
        return SQSProvider(
            queue_name=self.queue_name,
            aws_account_id=self.aws_account_id,
            aws_region=self.aws_region,
        )
