import pytest
import ray


@pytest.fixture(scope="function")
def ray_fix():
    ray.init(num_cpus=1, ignore_reinit_error=True)
    yield None
    ray.shutdown()
