import dataclasses
from buildflow.core.options.runtime_options import AutoscalerOptions, ProcessorOptions
from buildflow.io.endpoint import Method, Route

from buildflow.io.primitive import Primitive

from typing import Optional, Callable, Any


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
    autoscale_options: AutoscalerOptions = AutoscalerOptions.default(),
    log_level: str = "INFO",
):
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
