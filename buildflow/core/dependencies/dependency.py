import inspect
from contextlib import ExitStack, contextmanager
from typing import Any, Dict, Generic, Optional, Type, TypeVar

from buildflow.core.credentials import CredentialType
from buildflow.io.primitive import Primitive


class Dependency:
    def __init__(self, primitive: Primitive):
        self.primitive = primitive
        self.credentials: Optional[CredentialType] = None
        self.annotated_type: Optional[Type] = None

    def attach_credentials(self, credential_type: CredentialType) -> None:
        self.credentials = credential_type

    def attach_annotated_type(self, annotated_type: Type) -> None:
        self.annotated_type = annotated_type

    @contextmanager
    def materialize(self):
        raise NotImplementedError(
            f"materialize not implemented for {self.__class__.__name__}"
        )


class KwargDependencies:
    def __init__(self, arg_name_to_dependency: Dict[str, Dependency]):
        self.arg_name_to_dependency = arg_name_to_dependency

    @contextmanager
    def materialize(self):
        with ExitStack() as stack:
            kwargs = {
                arg_name: stack.enter_context(dependency.materialize())
                for arg_name, dependency in self.arg_name_to_dependency.items()
            }
            yield kwargs


P = TypeVar("P", bound=Primitive)


class Client(Generic[P], Dependency):
    def __init__(self, primitive: P):
        super().__init__(primitive)
        self.client: Optional[Any] = None

    @contextmanager
    def materialize(self):
        if self.client is None:
            if self.credentials is None:
                raise ValueError(
                    "Cannot materialize client without credentials attached"
                )
            provider = self.primitive.client_provider()
            self.client = provider.client(self.credentials, self.annotated_type)
        try:
            yield self.client
        finally:
            # Optionally, implement any teardown logic here
            pass


class Sink(Dependency):
    def __init__(self, primitive: Primitive):
        super().__init__(primitive)
        self.sink: Optional[Any] = None

    @contextmanager
    def materialize(self):
        if self.sink is None:
            if self.credentials is None:
                raise ValueError("Cannot materialize sink without credentials attached")
            self.sink = self.primitive.sink_provider().sink(self.credentials)
        try:
            yield self.sink
        finally:
            # Optionally, implement any teardown logic here
            pass
