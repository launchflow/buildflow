from enum import Enum
import inspect
from typing import Any, Callable, Dict, Optional, Type, TypeVar

from buildflow.api.depends import DependsPublisher


class Cloud(Enum):
    GCP = "gcp"
    AWS = "aws"


# TODO: Add Provider API for users who want to write their custom IO providers
# for the BuildFlow runtime.


class Pullable:

    def pull(self):
        raise NotImplementedError('pull not implemented')


class Pushable:

    def push(self):
        raise NotImplementedError('push not implemented')


class _BaseIO:

    @classmethod
    def num_cpus(cls) -> float:
        # IO options don't need much CPU pretty much just need enough keep it
        # scheduled.
        # TODO: we should probably make this configurable though to help
        # prevent OOMs.
        return 0.1

    def plan(self, process_arg_spec: inspect.FullArgSpec) -> Dict[str, Any]:
        return {"source_type": type(self).__name__}


class SourceType(_BaseIO):
    """Super class for all source types."""

    def setup(self):
        """Perform any setup that is needed to connect to a source."""

    def actor(self, ray_sinks):
        """Returns the actor associated with the source."""
        pass

    def preprocess(self, element: Any) -> Any:
        return element

    @classmethod
    def recommended_num_threads(cls):
        # Each class that implements RaySource can use this to suggest a good
        # number of threads to use when creating the source in the runtime.
        return 1

    @classmethod
    def is_streaming(cls) -> bool:
        return False


class StreamingSource(SourceType, DependsPublisher):

    def backlog(self) -> Optional[float]:
        """Returns an estimate of the backlog for the source.

        This method will be polled by our manager to determine if we need to
        scale up the number of actor replicas.
        """
        raise NotImplementedError(
            "backlog should be implemented for streaming sources.")

    @classmethod
    def is_streaming(cls) -> bool:
        return True


# SourceType = TypeVar("SourceType", bound=Source)


class Sink(_BaseIO):
    """Super class for all sink types."""

    def setup(self, process_arg_spec: inspect.FullArgSpec):
        """Perform any setup that is needed to connect to a sink.

        Args:
            process_arg_spec: The arg spec for the process function defined by
                the user.
        """

    def actor(self, remote_fn: Callable, is_streaming: bool):
        """Returns the actor associated with the sink."""
        pass


SinkType = TypeVar("SinkType", bound=Sink)
