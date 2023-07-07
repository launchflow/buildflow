from buildflow.api_v2.node.pattern.primitive.resource.provider.provider_api import (
    ProviderAPI,
)


class Request:
    pass


class Response:
    pass


class EndpointProvider(ProviderAPI):
    async def recv(self) -> Request:
        """Recv receives a request from the endpoint."""
        raise NotImplementedError("recv not implemented")

    async def send(self, resp: Response):
        """Send sends a response to the endpoint."""
        raise NotImplementedError("send not implemented")
