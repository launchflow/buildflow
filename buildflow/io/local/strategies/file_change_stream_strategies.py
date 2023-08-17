import dataclasses
from copy import deepcopy
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List, Type

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from buildflow.core.credentials import EmptyCredentials
from buildflow.core.types.shared_types import FilePath
from buildflow.io.strategies.source import AckInfo, PullResponse, SourceStrategy
from buildflow.io.utils.schemas import converters
from buildflow.types.local import FileChangeStreamEventType
from buildflow.types.portable import FileChangeEvent


class EventHandler(FileSystemEventHandler):
    """Adds captured events to a queue consumed by the strategy."""

    def __init__(
        self,
        event_queue: List,
        lock: RLock,
        event_types: Iterable[FileChangeStreamEventType],
    ):
        super().__init__()
        self.event_queue = event_queue
        self.lock = lock
        self.event_types = event_types

    def on_moved(self, event):
        super(EventHandler, self).on_moved(event)
        if FileChangeStreamEventType.MOVED in self.event_types:
            with self.lock:
                self.event_queue.append(event)

    def on_created(self, event):
        super(EventHandler, self).on_created(event)
        if FileChangeStreamEventType.CREATED in self.event_types:
            with self.lock:
                self.event_queue.append(event)

    def on_deleted(self, event):
        super(EventHandler, self).on_deleted(event)
        if FileChangeStreamEventType.DELETED in self.event_types:
            with self.lock:
                self.event_queue.append(event)

    def on_modified(self, event):
        super(EventHandler, self).on_modified(event)
        if FileChangeStreamEventType.MODIFIED in self.event_types:
            with self.lock:
                self.event_queue.append(event)


@dataclasses.dataclass
class LocalFileChangeEvent(FileChangeEvent):
    event_type: FileChangeStreamEventType

    @property
    def blob(self) -> bytes:
        if self.metadata["eventType"] == "deleted":
            raise ValueError("Can't fetch blob for `delete` event.")
        with open(self.metadata["srcPath"], "rb") as f:
            return f.read()


class LocalFileChangeStreamSource(SourceStrategy):
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
        self.watchdog_observer = Observer()
        self.event_types = event_types
        self.watchdog_observer.start()
        self.lock = RLock()
        self.event_queue = []
        self.event_handler = EventHandler(
            self.event_queue, lock=self.lock, event_types=event_types
        )
        self.watchdog_observer.schedule(self.event_handler, self.file_path)

    async def pull(self) -> PullResponse:
        with self.lock:
            events = deepcopy(self.event_queue)
            self.event_queue.clear()
        payloads = []
        for event in events:
            metadata = {
                "eventType": event.event_type,
                "isDirectory": event.is_directory,
                "srcPath": event.src_path,
                "isSynthetic": event.is_synthetic,
            }
            payloads.append(
                LocalFileChangeEvent(
                    file_path=event.src_path,
                    event_type=FileChangeStreamEventType(event.event_type),
                    metadata=metadata,
                )
            )
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
