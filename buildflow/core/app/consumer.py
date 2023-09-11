import dataclasses
from buildflow.core.options.runtime_options import AutoscalerOptions, ProcessorOptions

from buildflow.io.primitive import Primitive

from typing import Optional, Callable, Any


@dataclasses.dataclass
class Consumer:
    source_primitive: Primitive
    sink_primitive: Optional[Primitive]
    processor_options: ProcessorOptions
    original_process_fn_or_class: Callable

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.original_process_fn_or_class(*args, **kwargs)


def consumer(
    source: Primitive,
    sink: Optional[Primitive] = None,
    *,
    num_cpus: float = 1.0,
    num_concurrency: int = 1,
    autoscale_options: AutoscalerOptions = AutoscalerOptions.default(),
    log_level: str = "INFO",
):
    def decorator_function(original_fn_or_class):
        return Consumer(
            source_primitive=source,
            sink_primitive=sink,
            processor_options=ProcessorOptions(
                num_cpus=num_cpus,
                num_concurrency=num_concurrency,
                log_level=log_level,
                autoscaler_options=autoscale_options,
            ),
            original_process_fn_or_class=original_fn_or_class,
        )

    return decorator_function
