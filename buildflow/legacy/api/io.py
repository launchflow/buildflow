from enum import Enum


class Cloud(Enum):
    GCP = "gcp"
    AWS = "aws"


class IOBase:
    def provider(self):
        raise NotImplementedError("provider not implemented")


class SourceType(IOBase):
    """Super class for all source types."""


class SinkType(IOBase):
    """Super class for all sink types."""


class Pullable(SourceType):
    def pull(self):
        raise NotImplementedError("pull not implemented")


class Pushable(SinkType):
    def push(self):
        raise NotImplementedError("push not implemented")
