import dataclasses
from typing import Any, Callable, Optional

from buildflow.core.options.runtime_options import AutoscalerOptions, ProcessorOptions
from buildflow.io.primitive import Primitive


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
    enable_autoscaler: bool = True,
    num_replicas: int = 1,
    min_replicas: int = 1,
    max_replicas: int = 1000,
    autoscale_frequency_secs: int = 60,
    consumer_backlog_burn_threshold: int = 60,
    consumer_cpu_percent_target: int = 25,
    log_level: str = "INFO",
):
    autoscale_options = AutoscalerOptions(
        enable_autoscaler=enable_autoscaler,
        num_replicas=num_replicas,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        autoscale_frequency_secs=autoscale_frequency_secs,
        consumer_backlog_burn_threshold=consumer_backlog_burn_threshold,
        consumer_cpu_percent_target=consumer_cpu_percent_target,
    )

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
