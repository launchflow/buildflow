import inspect
from contextlib import ExitStack, contextmanager
from typing import Any, Dict, List, Optional, Type

from buildflow.core.credentials import CredentialType
from buildflow.io.primitive import Primitive


class Dependency:
    def __init__(self, primitive: Primitive):
        self.primitive = primitive
        self.credentials: Optional[CredentialType] = None

    def attach_credentials(self, credential_type: CredentialType) -> None:
        self.credentials = credential_type

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


# Python Magic - maybe not the best way to do this but should work as long as no
# threads change their annotations dynamically
class _AnnotationCapturer(type):
    captured_annotation: Optional[Type] = None

    def __call__(cls, *args, **kwargs):
        print("CALLED")
        frame = inspect.currentframe().f_back  # Capture the frame immediately
        instance = super().__call__(*args, **kwargs)
        print("INSTANCE", instance)
        for name, value in frame.f_locals.items():
            if value is instance:
                print("FOUND: ", name, value)
                annotations = frame.f_locals.get("__annotations__", {})
                instance.captured_annotation = annotations.get(name, None)
                break
        return instance


class Client(Dependency, metaclass=_AnnotationCapturer):
    def __init__(self, primitive: Primitive):
        super().__init__(primitive)
        self.captured_annotation: Optional[Type] = None
        self.client: Optional[Any] = None

    @contextmanager
    def materialize(self):
        if self.client is None:
            if self.credentials is None:
                raise ValueError(
                    "Cannot materialize client without credentials attached"
                )
            self.client = self.primitive.client_provider().client(
                self.credentials, self.captured_annotation
            )
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
