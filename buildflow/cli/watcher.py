import asyncio
import os
import signal
import sys
from multiprocessing import get_context
from typing import Callable, Optional

from watchfiles import awatch

from buildflow.cli import utils

# Use spawn context to make sure code run in subprocess
# does not reuse imported modules in main process/context
spawn_context = get_context("spawn")


def launch_debug_app(
    app: str,
    start_runtime_server: bool,
    run_id: str,
    runtime_server_host: str,
    runtime_server_port: int,
    serve_host: str,
    serve_port: int,
    event_subscriber: Optional[Callable],
):
    sys.path.insert(0, "")
    imported = utils.import_from_string(app)
    imported.run(
        start_runtime_server=start_runtime_server,
        runtime_server_host=runtime_server_host,
        runtime_server_port=runtime_server_port,
        run_id=run_id,
        debug_run=True,
        serve_host=serve_host,
        serve_port=serve_port,
        event_subscriber=event_subscriber,
    )


class RunTimeWatcher:
    def __init__(
        self,
        app: str,
        start_runtime_server: bool,
        run_id: str,
        runtime_server_host: str,
        runtime_server_port: int,
        serve_host: str,
        serve_port: int,
        event_subscriber: Optional[Callable],
    ) -> None:
        self.app = app
        self.start_runtime_server = start_runtime_server
        self.run_id = run_id
        self.runtime_server_host = runtime_server_host
        self.runtime_server_port = runtime_server_port
        self.serve_host = serve_host
        self.serve_port = serve_port
        self.event_subscriber = event_subscriber

    async def run(self):
        self.process = self.start_process()
        watch_task = asyncio.create_task(self.watch_for_changes())
        while True:
            if not self.process.is_alive():
                try:
                    watch_task.cancel()
                    await watch_task
                    return
                except asyncio.CancelledError:
                    return
            else:
                await asyncio.sleep(2)

    def start_process(self):
        process = spawn_context.Process(
            target=launch_debug_app,
            kwargs={
                "app": self.app,
                "start_runtime_server": self.start_runtime_server,
                "run_id": self.run_id,
                "runtime_server_host": self.runtime_server_host,
                "runtime_server_port": self.runtime_server_port,
                "serve_host": self.serve_host,
                "serve_port": self.serve_port,
                "event_subscriber": self.event_subscriber,
            },
        )
        process.start()
        return process

    async def watch_for_changes(self):
        async for _ in awatch("."):
            os.kill(self.process.pid, signal.SIGINT)
            self.process.join()
            self.process = self.start_process()
