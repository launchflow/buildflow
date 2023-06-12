from typing import Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from ray import serve

from buildflow.core.infra import PulumiInfraActor
from buildflow.core.runtime import RuntimeActor

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
"""


@serve.deployment(route_prefix="/", ray_actor_options={"num_cpus": 0.1})
@serve.ingress(app)
class NodeServer:
    def __init__(
        self,
        runtime_actor: RuntimeActor,
        infra_actor: Optional[PulumiInfraActor] = None,
    ) -> None:
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
        result = await self.runtime_actor.snapshot.remote()
        return result.as_dict()

    @app.get("/infra/snapshot")
    async def infra_snapshot(self):
        return "Not implemented yet."
