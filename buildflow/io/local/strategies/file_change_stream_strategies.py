import asyncio
import dataclasses
from threading import Lock
from typing import Any, Callable, Dict, Iterable, Type

from watchfiles import awatch

from buildflow.core.credentials import EmptyCredentials
from buildflow.core.types.shared_types import FilePath
from buildflow.io.strategies.source import AckInfo, PullResponse, SourceStrategy
from buildflow.io.utils.schemas import converters
from buildflow.types.local import FileChangeStreamEventType
from buildflow.types.portable import FileChangeEvent


@dataclasses.dataclass
class LocalFileChangeEvent(FileChangeEvent):
    event_type: FileChangeStreamEventType

    @property
    def blob(self) -> bytes:
        if self.event_type == FileChangeStreamEventType.DELETED:
            raise ValueError("Can't fetch blob for `delete` event.")
        with open(self.metadata["src_path"], "rb") as f:
            return f.read()


class LocalFileChangeStreamSource(SourceStrategy):
    _setup_task = None

    def __init__(
        self,
        *,
        credentials: EmptyCredentials,
        file_path: FilePath,
        event_types: Iterable[FileChangeStreamEventType],
    ):
        super().__init__(
            credentials=credentials, strategy_id="local-file-change-stream-source"
        )
        self.file_path = file_path
        self.event_types = {et.value for et in event_types}
        self.event_queue = []
        self.lock = Lock()

    async def setup(self):
        # TODO: we have to do this to ensure the observer isn't setup twice.
        self.event_queue = []
        async for change in awatch(self.file_path):
            for event, file_path in change:
                if event.name in self.event_types:
                    metadata = {"event_type": event, "src_path": file_path}
                    with self.lock:
                        self.event_queue.append(
                            LocalFileChangeEvent(
                                file_path=file_path,
                                event_type=FileChangeStreamEventType(event.name),
                                metadata=metadata,
                            )
                        )

    async def teardown(self):
        if self._setup_task is not None:
            self._setup_task.cancel()
            try:
                await self._setup_task
            except asyncio.CancelledError:
                pass

    async def pull(self) -> PullResponse:
        if self._setup_task is None:
            self._setup_task = asyncio.create_task(self.setup())
        payloads = []
        if self.event_queue:
            with self.lock:
                payloads.extend(self.event_queue)
                self.event_queue.clear()
        return PullResponse(payload=payloads, ack_info=None)

    def pull_converter(
        self, user_defined_type: Type
    ) -> Callable[[Dict[str, Any]], Any]:
        return converters.identity()

    async def backlog(self) -> int:
        return 0

    async def ack(self, to_ack: AckInfo, success: bool):
        return

    def max_batch_size(self) -> int:
        return -1
