from pathlib import Path
from socket import socket
from typing import Callable, List, Optional

from uvicorn.config import Config
from uvicorn.supervisors.basereload import BaseReload
from watchfiles import watch


class WatchFilesReload(BaseReload):
    def __init__(
        self,
        config: Config,
        target: Callable[[Optional[List[socket]]], None],
        sockets: List[socket],
    ) -> None:
        super().__init__(config, target, sockets)
        self.reloader_name = "WatchFiles"
        self.reload_dirs = []
        for directory in config.reload_dirs:
            if Path.cwd() not in directory.parents:
                self.reload_dirs.append(directory)
        if Path.cwd() not in self.reload_dirs:
            self.reload_dirs.append(Path.cwd())

        self.watcher = watch(
            *self.reload_dirs,
            watch_filter=None,
            stop_event=self.should_exit,
            # using yield_on_timeout here mostly to make sure tests don't
            # hang forever, won't affect the class's behavior
            yield_on_timeout=True,
        )

    def should_restart(self) -> Optional[List[Path]]:
        self.pause()

        changes = next(self.watcher)
        if changes:
            return {Path(c[1]) for c in changes}
        return None
