from typing import Iterable

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.dependencies.base import Scope, dependency
from buildflow.dependencies.flow_dependencies import FlowCredentials
from buildflow.io.primitive import Primitive, PrimitiveType
from buildflow.io.strategies.sink import SinkStrategy


def SinkDependencyBuilder(
    primitive: Primitive,
):
    PrimDep = primitive.dependency()

    @dependency(scope=Scope.REPLICA)
    class SinkDependency:
        def __init__(self, prim: PrimDep, flow_credentials: FlowCredentials):
            if prim.primitive_type == PrimitiveType.GCP:
                credentials = flow_credentials.gcp_credentials
            elif prim.primitive_type == PrimitiveType.AWS:
                credentials = flow_credentials.aws_credentials
            else:
                credentials = EmptyCredentials()
            self._sink: SinkStrategy = prim.sink(credentials)

        async def push(self, data):
            push_converter = self._sink.push_converter(type(data))
            converted_data = push_converter(data)
            await self._sink.push([converted_data])

        async def push_batch(self, batch: Iterable):
            if batch:
                push_converter = self._sink.push_converter(next(iter(batch)))
                converted_data = [push_converter(data) for data in batch]
                await self._sink.push(converted_data)

    return SinkDependency
