from typing import Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from ray import kill, serve

from buildflow.core.app.infra.actors.infra import InfraActor
from buildflow.core.app.runtime.actors.runtime import RuntimeActor
from buildflow.core.app.runtime.actors.runtime import RuntimeSnapshot
import logging

app = FastAPI()


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
                    console.log(response);
                    return response.json();
                })
                .then(json => document.getElementById('snapshot').innerHTML = JSON.stringify(json, null, 2));
        }
    </script>
    <body>
        <h1>Node Server UI</h1>
        <button onclick="drain()">Drain</button>
        <button onclick="snapshot()">Snapshot</button>
        <div>
            <pre id="snapshot"></pre>
        </div>
    </body>
</html>
"""  # noqa: E501


# Set this to avoid noisy logs for alls to each endpoint
logger = logging.getLogger("ray.serve")
logger.setLevel("WARNING")


@serve.deployment(route_prefix="/", ray_actor_options={"num_cpus": 0.1})
@serve.ingress(app)
class RuntimeServer:
    def __init__(
        self,
        runtime_actor: RuntimeActor,
        infra_actor: Optional[InfraActor] = None,
        *,
        log_level: str = "DEBUG",
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(log_level)

        # configuration
        self.runtime_actor = runtime_actor
        self.infra_actor = infra_actor

    @app.get("/")
    async def index(self):
        return HTMLResponse(index_html)

    @app.post("/runtime/drain")
    async def runtime_drain(self):
        # we dont want to block the request, so we dont await the drain
        self.runtime_actor.drain.remote()
        return "Drain request sent."

    @app.get("/runtime/snapshot")
    async def runtime_snapshot(self):
        snapshot: RuntimeSnapshot = await self.runtime_actor.snapshot.remote()
        return snapshot.as_dict()

    @app.post("/runtime/stop")
    async def runtime_stop(self):
        kill(self.runtime_actor)

    @app.get("/runtime/status")
    async def runtime_status(self):
        status = await self.runtime_actor.status.remote()
        return {"status": status.name}

    @app.get("/infra/snapshot")
    async def infra_snapshot(self):
        return "Not implemented yet."
