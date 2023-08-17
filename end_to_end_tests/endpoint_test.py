import asyncio
import dataclasses
import unittest

import pytest
import requests

import buildflow


@dataclasses.dataclass
class InputRequest:
    val: int


@dataclasses.dataclass
class OutputResponse:
    val: int


@pytest.mark.usefixtures("ray_fix")
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

        @app.endpoint(route="/test", method="POST", num_cpus=0.5)
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

        @app.endpoint(route="/test", method="POST", num_cpus=0.5)
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


if __name__ == "__main__":
    unittest.main()
