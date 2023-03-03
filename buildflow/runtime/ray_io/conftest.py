import pytest
import ray


@pytest.fixture(scope="package")
def ray_fix():
    ray.init(num_cpus=1)
    yield None
    ray.shutdown()
