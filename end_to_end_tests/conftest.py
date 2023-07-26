import asyncio

import pytest
import ray


@pytest.fixture(scope="function")
def ray_fix():
    ray.init(num_cpus=1, ignore_reinit_error=True)
    yield None
    ray.shutdown()


@pytest.fixture(scope="class")
def event_loop_instance(request):
    """Add the event_loop as an attribute to the unittest style test class."""
    request.cls.event_loop = asyncio.get_event_loop_policy().new_event_loop()
    yield
    request.cls.event_loop.close()
