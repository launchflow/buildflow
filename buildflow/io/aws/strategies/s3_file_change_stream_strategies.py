import json
from typing import Any, Callable, Coroutine, Type

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.io.aws.strategies.sqs_strategies import SQSSource
from buildflow.io.strategies.source import AckInfo, PullResponse, SourceStrategy
from buildflow.io.utils.clients.aws_clients import AWSClients
from buildflow.io.utils.schemas import converters
from buildflow.types.aws import S3ChangeStreamEventType, S3FileChangeEvent


class S3FileChangeStreamSource(SourceStrategy):
    def __init__(
        self,
        *,
        credentials: AWSCredentials,
        sqs_source: SQSSource,
        aws_region: str,
        filter_test_events: bool = True,
    ):
        super().__init__(
            credentials=credentials, strategy_id="aws-s3-filestream-source"
        )
        self.sqs_queue_source = sqs_source
        aws_clients = AWSClients(credentials=credentials, region=aws_region)
        self._s3_client = aws_clients.s3_client()
        self._filter_test_events = filter_test_events

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
                            event_type=s3_event_type,
                            metadata=record,
                        )
                    )
            else:
                if metadata.get("Event") == "s3:TestEvent" and self._filter_test_events:
                    # Filter out test events from S3 if specified, we ack it so it
                    # won't be delivered again.
                    if len(sqs_response.payload) == 1:
                        await self.sqs_queue_source.ack(sqs_response.ack_info, True)
                    continue
                # Otherwise, we don't know what this is, so we'll just pass it along
                parsed_payloads.append(
                    S3FileChangeEvent(
                        s3_client=self._s3_client,
                        bucket_name=metadata.get("Bucket"),
                        metadata=metadata,
                        file_path=None,
                        event_type=S3ChangeStreamEventType.UNKNOWN,
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
