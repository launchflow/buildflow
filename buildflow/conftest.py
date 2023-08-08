import asyncio
from typing import Any, Callable
from unittest.mock import MagicMock

import aiobotocore.awsrequest
import aiobotocore.endpoint
import aiohttp
import aiohttp.client_reqrep
import aiohttp.typedefs
import botocore.awsrequest
import botocore.model
import pytest
import ray


@pytest.fixture(scope="class")
def event_loop_instance(request):
    """Add the event_loop as an attribute to the unittest style test class."""
    request.cls.event_loop = asyncio.get_event_loop_policy().new_event_loop()
    yield
    request.cls.event_loop.close()


@pytest.fixture(scope="function")
def ray_fix():
    ray.init(num_cpus=1, ignore_reinit_error=True)
    yield None
    ray.shutdown()


# NOTE: The below mocks are to ensure s3fs works with moto
# See: https://github.com/aio-libs/aiobotocore/issues/755
# I only ran into this when testing on a mac FWIW.
class MockAWSResponse(aiobotocore.awsrequest.AioAWSResponse):
    """
    Mocked AWS Response.

    https://github.com/aio-libs/aiobotocore/issues/755
    https://gist.github.com/giles-betteromics/12e68b88e261402fbe31c2e918ea4168
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        self._moto_response = response
        self.status_code = response.status_code
        self.raw = MockHttpClientResponse(response)

    # adapt async methods to use moto's response
    async def _content_prop(self) -> bytes:
        return self._moto_response.content

    async def _text_prop(self) -> str:
        return self._moto_response.text


class MockHttpClientResponse(aiohttp.client_reqrep.ClientResponse):
    """
    Mocked HTP Response.

    See <MockAWSResponse> Notes
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        """
        Mocked Response Init.
        """

        async def read(self: MockHttpClientResponse, n: int = -1) -> bytes:
            return response.content

        self.content = MagicMock(aiohttp.StreamReader)
        self.content.read = read
        self.response = response

    @property
    def raw_headers(self) -> Any:
        """
        Return the headers encoded the way that aiobotocore expects them.
        """
        return {
            k.encode("utf-8"): str(v).encode("utf-8")
            for k, v in self.response.headers.items()
        }.items()


@pytest.fixture(scope="session", autouse=True)
def patch_aiobotocore() -> None:
    """
    Pytest Fixture Supporting S3FS Mocks.

    See <MockAWSResponse> Notes
    """

    def factory(original: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Any]:
        """
        Response Conversion Factory.
        """

        def patched_convert_to_response_dict(
            http_response: botocore.awsrequest.AWSResponse,
            operation_model: botocore.model.OperationModel,
        ) -> Any:
            return original(MockAWSResponse(http_response), operation_model)

        return patched_convert_to_response_dict

    aiobotocore.endpoint.convert_to_response_dict = factory(
        aiobotocore.endpoint.convert_to_response_dict
    )
