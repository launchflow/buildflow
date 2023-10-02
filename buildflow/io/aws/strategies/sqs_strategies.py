import asyncio
import dataclasses
from typing import Any, Callable, Iterable, List, Optional, Type

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.types.aws_types import AWSAccountID, AWSRegion, SQSQueueName
from buildflow.core.utils import uuid
from buildflow.io.strategies.sink import Batch, SinkStrategy
from buildflow.io.strategies.source import AckInfo, PullResponse, SourceStrategy
from buildflow.io.utils.clients.aws_clients import AWSClients
from buildflow.io.utils.schemas import converters

_MAX_BATCH_SIZE = 10


@dataclasses.dataclass(frozen=True)
class _MessageInfo:
    message_id: str
    receipt_handle: str


@dataclasses.dataclass
class _SQSAckInfo(AckInfo):
    message_infos: Iterable[_MessageInfo]


def _get_queue_url(
    aws_conn, queue_name: SQSQueueName, aws_account_id: Optional[AWSAccountID]
):
    if aws_account_id is None:
        response = aws_conn.get_queue_url(QueueName=queue_name)
    else:
        response = aws_conn.get_queue_url(
            QueueName=queue_name, QueueOwnerAWSAccountId=aws_account_id
        )
    return response["QueueUrl"]


class SQSSink(SinkStrategy):
    def __init__(
        self,
        credentials: AWSCredentials,
        queue_name: SQSQueueName,
        aws_account_id: Optional[AWSAccountID],
        aws_region: Optional[AWSRegion],
    ):
        super().__init__(credentials, "aws-sqs-sink")
        self.queue_name = queue_name
        self.aws_account_id = aws_account_id
        self.aws_region = aws_region
        aws_clients = AWSClients(credentials=credentials, region=self.aws_region)
        self.sqs_client = aws_clients.sqs_client()
        self.queue_url = _get_queue_url(
            self.sqs_client, self.queue_name, self.aws_account_id
        )

    def _send_messages(self, messages: List[str]):
        to_send = []
        for message in messages:
            to_send.append({"Id": uuid(80), "MessageBody": message})
        response = self.sqs_client.send_message_batch(
            QueueUrl=self.queue_url, Entries=to_send
        )
        if "Failed" in response:
            raise ValueError(f"failed to write messages to SQS: {response['Failed']}")

    async def push(self, batch: Batch):
        coros = []
        loop = asyncio.get_event_loop()
        for i in range(0, len(batch), _MAX_BATCH_SIZE):
            batch_to_write = batch[i : i + _MAX_BATCH_SIZE]
            coros.append(
                loop.run_in_executor(None, self._send_messages, batch_to_write)
            )

        return await asyncio.gather(*coros)

    def push_converter(self, user_defined_type: Optional[Type]) -> Callable[[Any], str]:
        return converters.str_push_converter(user_defined_type)


class SQSSource(SourceStrategy):
    def __init__(
        self,
        credentials: AWSCredentials,
        queue_name: SQSQueueName,
        aws_account_id: Optional[AWSAccountID],
        aws_region: Optional[AWSRegion],
    ):
        super().__init__(credentials, "aws-sqs-source")
        self.queue_name = queue_name
        self.aws_account_id = aws_account_id
        self.aws_region = aws_region
        aws_clients = AWSClients(credentials=credentials, region=self.aws_region)
        self.sqs_client = aws_clients.sqs_client()
        self.queue_url = _get_queue_url(
            self.sqs_client, self.queue_name, self.aws_account_id
        )

    def _pull(self) -> PullResponse:
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            AttributeNames=["All"],
            MaxNumberOfMessages=_MAX_BATCH_SIZE,
        )
        payload = []
        message_infos = []
        for message in response.get("Messages", []):
            message_info = _MessageInfo(
                message_id=message["MessageId"], receipt_handle=message["ReceiptHandle"]
            )
            message_infos.append(message_info)
            payload.append(message["Body"])
        return PullResponse(
            payload=payload, ack_info=_SQSAckInfo(message_infos=message_infos)
        )

    async def pull(self) -> PullResponse:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._pull)

    def _delete_messages(self, batch_to_delete: Iterable[_MessageInfo]):
        to_delete = []
        for info in batch_to_delete:
            to_delete.append(
                {"Id": info.message_id, "ReceiptHandle": info.receipt_handle}
            )
        response = self.sqs_client.delete_message_batch(
            QueueUrl=self.queue_url, Entries=to_delete
        )
        if "Failed" in response:
            raise ValueError(f"message delete failed: {response['Failed']}")

    async def ack(self, to_ack: _SQSAckInfo, success: bool):
        if success:
            coros = []
            loop = asyncio.get_event_loop()
            for i in range(0, len(to_ack.message_infos), _MAX_BATCH_SIZE):
                batch_to_delete = to_ack.message_infos[i : i + _MAX_BATCH_SIZE]
                coros.append(
                    loop.run_in_executor(None, self._delete_messages, batch_to_delete)
                )
            await asyncio.gather(*coros)

    def _get_backlog(self):
        queue_atts = self.sqs_client.get_queue_attributes(
            QueueUrl=self.queue_url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        if "ApproximateNumberOfMessages" in queue_atts["Attributes"]:
            return int(queue_atts["Attributes"]["ApproximateNumberOfMessages"])
        return 0

    async def backlog(self) -> int:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_backlog)

    def max_batch_size(self) -> int:
        return _MAX_BATCH_SIZE

    def pull_converter(self, type_: Type) -> Callable[[str], Any]:
        return converters.str_pull_converter(type_)
