import dataclasses
import json
from typing import Any, Callable, Coroutine, Type

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.io.aws.strategies.sqs_strategies import SQSSource
from buildflow.core.io.utils.clients.aws_clients import AWSClients
from buildflow.core.io.utils.schemas import converters
from buildflow.core.strategies.source import (AckInfo, PullResponse,
                                              SourceStrategy)
from buildflow.core.types.aws_types import (S3BucketName,
                                            S3ChangeStreamEventType)
from buildflow.core.types.portable_types import (FileChangeEvent,
                                                 PortableFileChangeEventType)


@dataclasses.dataclass
class S3FileChangeEvent(FileChangeEvent):
    bucket_name: S3BucketName
    s3_client: Any

    @property
    def blob(self) -> bytes:
        data = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_path)
        return data["Body"].read()


class S3FileChangeStreamSource(SourceStrategy):
    def __init__(
        self,
        *,
        credentials: AWSCredentials,
        sqs_source: SQSSource,
        aws_region: str,
    ):
        super().__init__(
            credentials=credentials, strategy_id="aws-s3-filestream-source"
        )
        self.sqs_queue_source = sqs_source
        aws_clients = AWSClients(credentials=credentials, region=aws_region)
        self._s3_client = aws_clients.s3_client()

    async def pull(self) -> PullResponse:
        sqs_response = await self.sqs_queue_source.pull()
        parsed_payloads = []
        for payload in sqs_response.payload:
            metadata = json.loads(payload)
            if "Records" in metadata:
                for record in metadata["Records"]:
                    file_path = record["s3"]["object"]["key"]
                    bucket_name = record["s3"]["bucket"]["name"]
                    s3_event_type = S3ChangeStreamEventType(record["eventName"])
                    parsed_payloads.append(
                        S3FileChangeEvent(
                            bucket_name=bucket_name,
                            s3_client=self._s3_client,
                            file_path=file_path,
                            portable_event_type=s3_event_type.to_portable_type(),
                            metadata=record,
                        )
                    )
            else:
                # This can happen for the initial test notification that is sent
                # on the queue.
                parsed_payloads.append(
                    S3FileChangeEvent(
                        s3_client=self._s3_client,
                        bucket_name=metadata.get("Bucket"),
                        metadata=metadata,
                        file_path=None,
                        portable_event_type=PortableFileChangeEventType.UNKNOWN,
                    )
                )
        return PullResponse(parsed_payloads, sqs_response.ack_info)

    def pull_converter(self, user_defined_type: Type) -> Callable[[Any], Any]:
        return converters.identity()

    async def backlog(self) -> Coroutine[Any, Any, int]:
        return await self.sqs_queue_source.backlog()

    async def ack(self, to_ack: AckInfo, success: bool):
        return await self.sqs_queue_source.ack(to_ack, success)

    def max_batch_size(self) -> int:
        return self.sqs_queue_source.max_batch_size()
