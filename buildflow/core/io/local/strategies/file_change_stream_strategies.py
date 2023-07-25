from copy import deepcopy
import dataclasses
from threading import RLock
from typing import Any, Callable, Dict, Iterable, List, Type

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from buildflow.core.credentials import EmptyCredentials
from buildflow.core.io.utils.schemas import converters
from buildflow.core.strategies.source import AckInfo, PullResponse, SourceStrategy
from buildflow.core.types.local_types import FileChangeStreamEvents
from buildflow.core.types.portable_types import (
    FileChangeEvent,
    PortableFileChangeEventType,
)
from buildflow.core.types.shared_types import FilePath


def _watchdog_event_to_portable_event_type(
    watchdog_event_type: str,
) -> PortableFileChangeEventType:
    if watchdog_event_type == "created":
        return PortableFileChangeEventType.CREATED
    elif watchdog_event_type == "deleted":
        return PortableFileChangeEventType.DELETED
    return PortableFileChangeEventType.UNKNOWN


class EventHandler(FileSystemEventHandler):
    """Logs all the events captured."""

    def __init__(
        self,
        event_queue: List,
        lock: RLock,
        event_types: Iterable[FileChangeStreamEvents],
    ):
        super().__init__()
        self.event_queue = event_queue
        self.lock = lock
        self.event_types = event_types

    def on_moved(self, event):
        super(EventHandler, self).on_moved(event)
        if FileChangeStreamEvents.MOVED in self.event_types:
            with self.lock:
                self.event_queue.append(event)

    def on_created(self, event):
        super(EventHandler, self).on_created(event)
        if FileChangeStreamEvents.CREATED in self.event_types:
            with self.lock:
                self.event_queue.append(event)

    def on_deleted(self, event):
        super(EventHandler, self).on_deleted(event)
        if FileChangeStreamEvents.DELETED in self.event_types:
            with self.lock:
                self.event_queue.append(event)

    def on_modified(self, event):
        super(EventHandler, self).on_modified(event)
        if FileChangeStreamEvents.MODIFIED in self.event_types:
            with self.lock:
                self.event_queue.append(event)


@dataclasses.dataclass
class LocalFileChangeEvent(FileChangeEvent):
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
        event_types: Iterable[FileChangeStreamEvents],
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
                    portable_event_type=_watchdog_event_to_portable_event_type(
                        event.event_type
                    ),
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
