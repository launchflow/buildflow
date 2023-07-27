from typing import List, Optional, Type
from buildflow.core.providers.provider import (
    PulumiProvider,
    SinkProvider,
    SourceProvider,
)
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.strategies.sink import SinkStrategy
from buildflow.core.strategies.source import SourceStrategy
from buildflow.core.types.aws_types import AWSAccountID, AWSRegion, QueueName
from buildflow.core.io.aws.strategies.sqs_strategies import SQSSink, SQSSource


class SQSProvider(PulumiProvider, SinkProvider, SourceProvider):
    def __init__(
        self,
        queue_name: QueueName,
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
        self, type_: Optional[Type], depends_on: List[PulumiResource] = ...
    ) -> List[PulumiResource]:
        # TODO: implement this
        pass
