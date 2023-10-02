import contextlib
import logging
import threading
import time
from typing import List, Optional

import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ray import kill

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
        allowed_google_ids: List[str] = (),
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
        self.allowed_google_ids = allowed_google_ids
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

    def _auth_request(self, request: Request):
        if self.allowed_google_ids:
            from google.auth.transport import requests
            from google.oauth2 import id_token

            auth_header = request.headers.get("Authorization")
            if auth_header is None:
                raise HTTPException(401)
            _, _, token = auth_header.partition(" ")
            id_info = id_token.verify_token(id_token, request=requests.Request())
            if (
                not id_info["email_verified"]
                or id_info["sub"] not in self.allowed_google_ids
            ):
                raise HTTPException(403)

    async def index(self, request: Request):
        self._auth_request(request)
        return HTMLResponse(index_html)

    async def runtime_drain(self, request: Request):
        self._auth_request(request)
        # we dont want to block the request, so we dont await the drain
        self.runtime_actor.drain.remote()
        return "Drain request sent."

    async def runtime_snapshot(self, request: Request):
        self._auth_request(request)
        snapshot: RuntimeSnapshot = await self.runtime_actor.snapshot.remote()
        return JSONResponse(snapshot.as_dict())

    async def runtime_stop(self, request: Request):
        self._auth_request(request)
        kill(self.runtime_actor)

    async def runtime_status(self, request: Request):
        self._auth_request(request)
        status = await self.runtime_actor.status.remote()
        return {"status": status.name}

    async def infra_snapshot(self, request: Request):
        self._auth_request(request)
        return "Not implemented yet."

    async def flowstate(self, request: Request):
        self._auth_request(request)
        return JSONResponse(self.flow_state)
