"""Mix-in classes for buildflow API objects."""


class Publisher:
    def publish(element):
        raise NotImplementedError("publish() must be implemented by Publisher")


class DependsPublisher:
    """Mix-in that indicates a source is a PubSub for a Depends object."""

    def publisher(self) -> Publisher:
        raise NotImplementedError("publisher() must be implemented by DependsPublisher")
