import asyncio
import dataclasses
import os
import tempfile
import unittest

import pytest
import requests
from websockets.sync.client import connect

import buildflow
from buildflow.dependencies.base import Scope, dependency
from buildflow.dependencies.sink import SinkDependencyBuilder
from buildflow.io.local.file import File
from buildflow.requests import Request, WebSocket
from buildflow.types.portable import FileFormat


@dataclasses.dataclass
class InputRequest:
    val: int


@dataclasses.dataclass
class OutputResponse:
    val: int


@pytest.mark.usefixtures("ray")
@pytest.mark.usefixtures("event_loop_instance")
class EndpointLocalTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    def run_for_time(self, coro, time: int = 5):
        async def wait_wrapper():
            completed, pending = await asyncio.wait(
                [coro], timeout=time, return_when="FIRST_EXCEPTION"
            )
            if completed:
                # This general should only happen when there was an exception so
                # we want to raise it to make the test failure more obvious.
                completed.pop().result()
            if pending:
                return pending.pop()

        return self.event_loop.run_until_complete(wait_wrapper())

    def test_endpoint_end_to_end(self):
        app = buildflow.Flow()
        service = app.service(num_cpus=0.5)

        @service.endpoint(route="/test", method="POST")
        def my_endpoint(input: InputRequest) -> OutputResponse:
            return OutputResponse(input.val + 1)

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = self.run_for_time(run_coro, time=20)

        response = requests.post(
            "http://0.0.0.0:8000/test", json={"val": 1}, timeout=10
        )
        response.raise_for_status()
        self.assertEqual(response.json(), {"val": 2})

        self.get_async_result(app._drain())

    def test_endpoint_end_to_end_class(self):
        app = buildflow.Flow()
        service = app.service(num_cpus=0.5)

        @service.endpoint(route="/test", method="POST")
        class Endpoint:
            def setup(self):
                self.val_to_add = 10

            def get_val_to_add(self):
                return self.val_to_add

            def process(self, input: InputRequest) -> OutputResponse:
                return OutputResponse(input.val + self.get_val_to_add())

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = self.run_for_time(run_coro, time=20)

        response = requests.post(
            "http://0.0.0.0:8000/test", json={"val": 1}, timeout=10
        )
        response.raise_for_status()
        self.assertEqual(response.json(), {"val": 11})

        self.get_async_result(app._drain())

    def test_endpoint_end_to_end_detached(self):
        app = buildflow.Flow()
        service = buildflow.Service(num_cpus=0.5)
        app.add_service(service)

        @service.endpoint(route="/test", method="POST")
        def my_endpoint(input: InputRequest) -> OutputResponse:
            return OutputResponse(input.val + 1)

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = self.run_for_time(run_coro, time=20)

        response = requests.post(
            "http://0.0.0.0:8000/test", json={"val": 1}, timeout=10
        )
        response.raise_for_status()
        self.assertEqual(response.json(), {"val": 2})

        self.get_async_result(app._drain())

    def test_endpoint_dependencies(self):
        @dependency(scope=Scope.NO_SCOPE)
        class NoScope:
            def __init__(self):
                self.val = 1

        @dependency(scope=Scope.GLOBAL)
        class GlobalScope:
            def __init__(self, no: NoScope):
                self.val = 2
                self.no = no

        @dependency(scope=Scope.REPLICA)
        class ReplicaScope:
            def __init__(self, global_: GlobalScope):
                self.val = 3
                self.global_ = global_

        @dependency(scope=Scope.PROCESS)
        class ProcessScope:
            def __init__(self, replica: ReplicaScope):
                self.val = 4
                self.replica = replica

        with tempfile.TemporaryDirectory() as output_dir:
            sink = File(
                os.path.join(output_dir, "file.csv"), file_format=FileFormat.CSV
            )
            SinkSideOutput = SinkDependencyBuilder(sink)

            app = buildflow.Flow()
            service = app.service(num_cpus=0.5)

            @service.endpoint(route="/test", method="POST")
            async def my_endpoint(
                input: InputRequest,
                no: NoScope,
                global_: GlobalScope,
                replica: ReplicaScope,
                process: ProcessScope,
                sink: SinkSideOutput,
            ) -> OutputResponse:
                if id(process.replica) != id(replica):
                    raise Exception("Replica scope not the same")
                if id(replica.global_) != id(global_):
                    raise Exception("Global scope not the same")
                if id(global_.no) == id(no):
                    raise Exception("No scope was the same")
                to_write = input.val + no.val + global_.val + replica.val + process.val
                to_return = OutputResponse(to_write)
                await sink.push(to_return)
                return OutputResponse(to_write)

            run_coro = app.run(block=False)

            # wait for 20 seconds to let it spin up
            run_coro = self.run_for_time(run_coro, time=20)

            response = requests.post(
                "http://0.0.0.0:8000/test", json={"val": 1}, timeout=10
            )
            response.raise_for_status()
            self.assertEqual(response.json(), {"val": 11})

            output_files = os.listdir(output_dir)
            self.assertEqual(len(output_files), 1)
            output_file = os.path.join(output_dir, output_files[0])

            with open(output_file, "r") as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 2)
                self.assertEqual(lines[0], '"val"\n')
                self.assertEqual(lines[1], "11\n")

            self.get_async_result(app._drain())

    def test_endpoint_with_default(self):
        app = buildflow.Flow()
        service = app.service(num_cpus=0.5)

        @service.endpoint(route="/test", method="GET")
        def my_endpoint(input: int = 1) -> OutputResponse:
            return OutputResponse(input + 1)

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = self.run_for_time(run_coro, time=20)

        response = requests.get("http://0.0.0.0:8000/test", timeout=10)
        response.raise_for_status()
        self.assertEqual(response.json(), {"val": 2})

        response = requests.get("http://0.0.0.0:8000/test?input=2", timeout=10)
        response.raise_for_status()
        self.assertEqual(response.json(), {"val": 3})

        self.get_async_result(app._drain())

    def test_endpoint_with_request(self):
        app = buildflow.Flow()
        service = app.service(num_cpus=0.5)

        @service.endpoint(route="/test", method="GET")
        def my_endpoint(request: Request, input: int = 1) -> OutputResponse:
            return OutputResponse(input + 1)

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = self.run_for_time(run_coro, time=20)

        response = requests.get("http://0.0.0.0:8000/test", timeout=10)
        response.raise_for_status()
        self.assertEqual(response.json(), {"val": 2})

        response = requests.get("http://0.0.0.0:8000/test?input=2", timeout=10)
        response.raise_for_status()
        self.assertEqual(response.json(), {"val": 3})

        self.get_async_result(app._drain())

    def test_endpoint_websocket(self):
        app = buildflow.Flow()
        service = app.service(num_cpus=0.5)

        @dependency(scope=Scope.PROCESS)
        class WebSocketDeb:
            def __init__(self, ws: WebSocket):
                self.header = ws.headers["test"]

        @service.endpoint(route="/test", method="websocket")
        async def my_endpoint(websocket: WebSocket, deb: WebSocketDeb):
            await websocket.accept()
            while True:
                message = await websocket.receive_text()
                await websocket.send_text(f"{message}, {deb.header}")

        run_coro = app.run(block=False)

        # wait for 20 seconds to let it spin up
        run_coro = self.run_for_time(run_coro, time=20)

        with connect(
            "ws://127.0.0.1:8000/test", additional_headers={"test": "world!"}
        ) as ws:
            ws.send("Hello")
            message = ws.recv()
            self.assertEqual(message, "Hello, world!")

        self.get_async_result(app._drain())


if __name__ == "__main__":
    unittest.main()
