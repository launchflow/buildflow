import inspect
from typing import Any, Callable, Dict, Union

from buildflow.api import io
from buildflow.runtime.ray_io import bigquery_io


class DataWarehouseSink(io.Sink):
    def __init__(self, cloud: Union[str, io.Cloud], name: str, **kwargs) -> None:
        self.name = name
        self.cloud_args = kwargs
        self.cloud = cloud
        if isinstance(self.cloud, str):
            self.cloud = io.Cloud(self.cloud)
        if self.cloud == io.Cloud.GCP:
            try:
                project = self.cloud_args["project_id"]
                del self.cloud_args["project_id"]
            except KeyError:
                raise ValueError("Missing required GCP argument: project_id")
            if "dataset" in self.cloud_args:
                dataset = self.cloud_args["dataset"]
                del self.cloud_args["dataset"]
            else:
                dataset = "buildflow_default"
            self.cloud_args["table_id"] = f"{project}.{dataset}.{self.name}"
            self._cloud_sink = bigquery_io.BigQuerySink(**self.cloud_args)
        else:
            raise ValueError(f"Unsupported cloud: {self.cloud}")

    def plan(self, process_arg_spec: inspect.FullArgSpec) -> Dict[str, Any]:
        return self._cloud_sink.plan(process_arg_spec)

    def setup(self, process_arg_spec: inspect.FullArgSpec):
        return self._cloud_sink.setup(process_arg_spec)

    def actor(self, remote_fn: Callable, is_streaming: bool):
        return bigquery_io.BigQuerySinkActor.remote(
            remote_fn, self._cloud_sink, is_streaming
        )
