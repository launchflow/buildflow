import dataclasses
from typing import Dict

from buildflow.core.options._options import Options
from buildflow.core.processor.processor import ProcessorID


@dataclasses.dataclass
class AutoscalerOptions(Options):
    """Options for the autoscaler.
    enable_autoscaler (bool): Whether or not autoscaling should be enabled.
        Defaults to True.
    min_replicas (int): The minimum number of replicas to scale down to.
        Defaults to 1.
    max_replicas (int): The maximum number of replicas to scale up to.
        Defaults to 1000.
    pipeline_backlog_burn_threshold (int): The threshold for how long it will
        take to burn down a backlog before scaling up. Increasing this number
        will cause your pipeline to scale up more aggresively. Defaults to 60.
    pipeline_cpu_percent_target (int): The target cpu percentage for scaling
        down. Increasing this number will cause your pipeline to scale down
        more aggresively. Defaults to 25.
    """

    enable_autoscaler: bool
    num_replicas: int
    min_replicas: int
    max_replicas: int
    autoscale_frequency_secs: int = 60
    # Options for configuring scaling for pipelines
    pipeline_backlog_burn_threshold: int = 60
    pipeline_cpu_percent_target: int = 25
    # Options for configuring scaling for collectors
    target_num_ongoing_requests_per_replica: int = 10

    @classmethod
    def default(cls) -> "AutoscalerOptions":
        return cls(
            enable_autoscaler=True,
            num_replicas=1,
            min_replicas=1,
            max_replicas=1000,
        )

    def __post_init__(self):
        if (
            self.pipeline_cpu_percent_target < 0
            or self.pipeline_cpu_percent_target > 100
        ):
            raise ValueError("pipeline_cpu_percent_target must be between 0 and 100")


# TODO: Add options for other pattern types, or merge into a single options object
@dataclasses.dataclass
class ProcessorOptions(Options):
    num_cpus: float
    num_concurrency: int
    log_level: str
    # the configuration of the autoscaler for this processor
    autoscaler_options: AutoscalerOptions

    @classmethod
    def default(cls) -> "ProcessorOptions":
        return cls(
            num_cpus=1.0,
            num_concurrency=1,
            log_level="INFO",
            autoscaler_options=AutoscalerOptions.default(),
        )


@dataclasses.dataclass
class RuntimeOptions(Options):
    # the configuration of each processor
    processor_options: Dict[ProcessorID, ProcessorOptions]
    # the number of replicas to start with (will scale up/down if autoscale is enabled)
    num_replicas: int
    # misc
    log_level: str
    checkin_frequency_loop_secs: int = 5

    @classmethod
    def default(cls) -> "RuntimeOptions":
        return cls(
            processor_options={},
            num_replicas=1,
            log_level="INFO",
        )
