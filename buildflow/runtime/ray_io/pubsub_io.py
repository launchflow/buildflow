import inspect
from typing import Callable, Optional, Type, Union

from buildflow.api import io
from buildflow.api.depends import Publisher
from buildflow.runtime.ray_io import gcp_pubsub_io


class PubSubSource(io.StreamingSource):
    def __init__(self, cloud: Union[str, io.Cloud], name: str, **kwargs) -> None:
        self.name = name
        self.cloud_args = kwargs
        self.cloud = cloud
        if isinstance(self.cloud, str):
            self.cloud = io.Cloud(self.cloud)
        if self.cloud == io.Cloud.GCP:
            try:
                project = self.cloud_args["project_id"]
            except KeyError:
                raise ValueError("Missing required GCP argument: project_id")
            del self.cloud_args["project_id"]
            self.cloud_args[
                "subscription"
            ] = f"projects/{project}/subscriptions/{self.name}"
            self.cloud_args["topic"] = f"projects/{project}/topics/{self.name}"
            self._cloud_source = gcp_pubsub_io.GCPPubSubSource(**self.cloud_args)
        else:
            raise ValueError(f"Unsupported cloud: {self.cloud}")

    def setup(self):
        return self._cloud_source.setup()

    def backlog(self) -> Optional[float]:
        return self._cloud_source.backlog()

    def actor(self, ray_sinks, proc_input_type: Optional[Type]):
        if self.cloud == io.Cloud.GCP:
            return gcp_pubsub_io.PubSubSourceActor.remote(
                ray_sinks, self._cloud_source, proc_input_type
            )
        else:
            raise ValueError(f"Unsupported cloud: {self.cloud}")

    def publisher(self) -> Publisher:
        return self._cloud_source.publisher()


class PubSubSink(io.Sink):
    def __init__(self, cloud: Union[str, io.Cloud], name: str, **kwargs) -> None:
        self.name = name
        self.cloud_args = kwargs
        self.cloud = cloud
        if isinstance(self.cloud, str):
            self.cloud = io.Cloud(self.cloud)
        if self.cloud == io.Cloud.GCP:
            try:
                project = self.cloud_args["project_id"]
            except KeyError:
                raise ValueError("Missing required GCP argument: project_id")
            del self.cloud_args["project_id"]
            self.cloud_args["topic"] = f"projects/{project}/topics/{self.name}"
            self._cloud_sink = gcp_pubsub_io.GCPPubSubSink(**self.cloud_args)
        else:
            raise ValueError(f"Unsupported cloud: {self.cloud}")

    def setup(self, process_arg_spec: inspect.FullArgSpec):
        return self._cloud_sink.setup(process_arg_spec)

    def actor(self, remote_fn: Callable, is_streaming: bool):
        return gcp_pubsub_io.PubSubSinkActor.remote(remote_fn, self._cloud_sink)
