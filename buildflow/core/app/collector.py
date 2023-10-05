import dataclasses
from typing import Any, Callable, Optional

from buildflow.core.options.runtime_options import AutoscalerOptions, ProcessorOptions
from buildflow.io.endpoint import Method, Route
from buildflow.io.primitive import Primitive


@dataclasses.dataclass
class Collector:
    route: Route
    method: Method
    sink_primitive: Optional[Primitive]
    processor_options: ProcessorOptions
    original_process_fn_or_class: Callable

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.original_process_fn_or_class(*args, **kwargs)


def collector(
    route: Route,
    method: Method,
    sink: Optional[Primitive] = None,
    *,
    num_cpus: float = 1.0,
    enable_autoscaler: bool = True,
    num_replicas: int = 1,
    min_replicas: int = 1,
    max_replicas: int = 1000,
    target_num_ongoing_requests_per_replica: int = 80,
    log_level: str = "INFO",
):
    autoscale_options = AutoscalerOptions(
        enable_autoscaler=enable_autoscaler,
        num_replicas=num_replicas,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        target_num_ongoing_requests_per_replica=target_num_ongoing_requests_per_replica,
    )

    def decorator_function(original_fn_or_class):
        return Collector(
            route=route,
            method=method,
            sink_primitive=sink,
            processor_options=ProcessorOptions(
                num_cpus=num_cpus,
                log_level=log_level,
                autoscaler_options=autoscale_options,
                # This option isn't used by collectors or endpoints
                num_concurrency=1,
            ),
            original_process_fn_or_class=original_fn_or_class,
        )

    return decorator_function
