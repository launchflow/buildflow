from copy import deepcopy
from threading import RLock
from typing import Any, Callable, Dict, List, Type

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from buildflow.core.credentials import EmptyCredentials
from buildflow.core.io.utils.schemas import converters
from buildflow.core.strategies.source import AckInfo, PullResponse, SourceStrategy
from buildflow.core.types.local_types import FilePath


class EventHandler(FileSystemEventHandler):
    """Logs all the events captured."""

    def __init__(self, event_queue: List, lock: RLock):
        super().__init__()
        self.event_queue = event_queue
        self.lock = lock

    def on_moved(self, event):
        super(EventHandler, self).on_moved(event)

    def on_created(self, event):
        super(EventHandler, self).on_created(event)
        with self.lock:
            self.event_queue.append(event)

    def on_deleted(self, event):
        super(EventHandler, self).on_deleted(event)

    def on_modified(self, event):
        super(EventHandler, self).on_modified(event)


class LocalFileChangeStreamSource(SourceStrategy):
    def __init__(
        self,
        *,
        credentials: EmptyCredentials,
        file_path: FilePath,
    ):
        super().__init__(
            credentials=credentials, strategy_id="local-file-change-stream-source"
        )
        self.file_path = file_path
        self.watchdog_observer = Observer()
        self.watchdog_observer.start()
        self.pending_events = []
        self.lock = RLock()
        self.event_queue = []
        self.event_handler = EventHandler(self.event_queue, lock=self.lock)
        self.watchdog_observer.schedule(self.event_handler, self.file_path)

    async def pull(self) -> PullResponse:
        with self.lock:
            to_return = deepcopy(self.event_queue)
            self.event_queue.clear()
        print("DO NOT SUBMIT: ", to_return)
        return PullResponse(payload=to_return, ack_info=None)

    def pull_converter(
        self, user_defined_type: Type
    ) -> Callable[[Dict[str, Any]], Any]:
        return converters.identity()

    async def backlog(self) -> int:
        return 0

    async def ack(self, to_ack: AckInfo, success: bool):
        return
