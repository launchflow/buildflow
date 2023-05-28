from typing import TypeVar, Generic

from buildflow.api import io
from buildflow.core.ray_io import gcp_pubsub_io, pubsub_io

T = TypeVar("T")


class UnsupportDepenendsSource(Exception):

    def __init__(self, source: io.Source):
        super().__init__(
            f"Depends is not supported for sources of type: {type(source)}")


class InvalidProcessorSource(Exception):

    def __init__(self) -> None:
        super().__init__("Could not determine source for processor. "
                         "Please use @classmethod source() or "
                         "source=... in the @processor decorator.")


class PubSub(Generic[T]):

    def __init__(self, source: io.Source) -> None:
        self.source = source
        self.publisher = None

    def publish(self, element: T):
        if self.publisher is None:
            self.publisher = self.source.publisher()
        self.publisher.publish(element)


def Depends(depends):
    if isinstance(depends,
                  (pubsub_io.PubSubSource, gcp_pubsub_io.GCPPubSubSource)):
        return PubSub(depends)
    raise UnsupportDepenendsSource(depends)
