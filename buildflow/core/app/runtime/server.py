import contextlib
import logging
import threading
import time
from typing import Optional

import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from buildflow.core.app.flow_state import FlowState
from buildflow.core.app.infra.actors.infra import InfraActor
from buildflow.core.app.runtime.actors.runtime import RuntimeActor, RuntimeSnapshot

app = FastAPI(
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


index_html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Buildflow</title>
    </head>
    <script>
        function drain() {
            fetch('/runtime/drain', {method: 'POST'})
                .then(response => response.text())
                .then(text => alert(text));
        }
        function snapshot() {
            fetch('/runtime/snapshot')
                .then(response => {
                    return response.json();
                })
                .then(json => document.getElementById('snapshot').innerHTML = JSON.stringify(json, null, 2));
        }
    </script>
    <body>
        <h1>RuntimeServer Server UI</h1>
        <button onclick="drain()">Drain</button>
        <button onclick="snapshot()">Snapshot</button>
        <div>
            <pre id="snapshot"></pre>
        </div>
    </body>
</html>
"""  # noqa: E501


class RuntimeServer(uvicorn.Server):
    def __init__(
        self,
        runtime_actor: RuntimeActor,
        host: str,
        port: int,
        flow_state: FlowState,
        infra_actor: Optional[InfraActor] = None,
        *,
        log_level: str = "WARNING",
    ) -> None:
        super().__init__(
            uvicorn.Config(app, host=host, port=port, log_level=log_level.lower())
        )
        self.flow_state = flow_state.to_dict()
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(log_level)

        # configuration
        self.runtime_actor = runtime_actor
        self.infra_actor = infra_actor
        self.router = APIRouter()
        self.router.add_api_route("/", self.index, methods=["GET"])
        self.router.add_api_route(
            "/runtime/drain", self.runtime_drain, methods=["POST"]
        )
        self.router.add_api_route("/runtime/stop", self.runtime_stop, methods=["POST"])
        self.router.add_api_route(
            "/runtime/snapshot", self.runtime_snapshot, methods=["GET"]
        )
        self.router.add_api_route(
            "/runtime/status", self.runtime_status, methods=["GET"]
        )
        self.router.add_api_route("/infra/status", self.runtime_status, methods=["GET"])
        self.router.add_api_route("/flowstate", self.flowstate, methods=["GET"])
        app.include_router(self.router)

    @contextlib.contextmanager
    def run_in_thread(self):
        thread = threading.Thread(target=self.run)
        thread.start()
        try:
            while not self.started:
                time.sleep(1e-3)
            yield
        finally:
            self.should_exit = True
            thread.join()

    async def index(self):
        return HTMLResponse(index_html)

    async def runtime_drain(self):
        # we dont want to block the request, so we dont await the drain
        self.runtime_actor.drain.remote()
        return "Drain request sent."

    async def runtime_snapshot(self):
        snapshot: RuntimeSnapshot = await self.runtime_actor.snapshot.remote()
        return JSONResponse(snapshot.as_dict())

    async def runtime_stop(self):
        # Send two drain requests to stop the runtime.
        self.runtime_actor.drain.remote()
        self.runtime_actor.drain.remote()

    async def runtime_status(self):
        status = await self.runtime_actor.status.remote()
        return {"status": status.name}

    async def infra_snapshot(self):
        return "Not implemented yet."

    async def flowstate(self):
        return JSONResponse(self.flow_state)
