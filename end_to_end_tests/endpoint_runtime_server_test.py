import asyncio
import dataclasses
import unittest
from multiprocessing import Process

import pytest
import requests

import buildflow


@dataclasses.dataclass
class InputRequest:
    val: int


@dataclasses.dataclass
class OutputResponse:
    val: int


def run_flow():
    app = buildflow.Flow()
    service = app.service(num_cpus=0.1)

    @service.endpoint(route="/test", method="POST")
    def my_endpoint(input: InputRequest) -> OutputResponse:
        return OutputResponse(input.val + 1)

    app.run(start_runtime_server=True)


@pytest.mark.ray
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

    def test_endpoint_end_to_end_with_runtime_server(self):
        try:
            p = Process(target=run_flow)
            p.start()

            # wait for 20 seconds to let it spin up
            self.get_async_result(asyncio.sleep(60))

            response = requests.post(
                "http://127.0.0.1:8000/test", json={"val": 1}, timeout=10
            )
            response.raise_for_status()
            self.assertEqual(response.json(), {"val": 2})

            response = requests.get(
                "http://127.0.0.1:9653/runtime/snapshot", timeout=10
            )
            response.raise_for_status()

            self.assertEqual(response.json()["status"], "RUNNING")

            response = requests.post("http://127.0.0.1:9653/runtime/drain", timeout=10)
            response.raise_for_status()

        finally:
            p.join(timeout=20)
            if p.is_alive():
                p.kill()
                p.join()


if __name__ == "__main__":
    unittest.main()
