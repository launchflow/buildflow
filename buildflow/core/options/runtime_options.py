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
    consumer_backlog_burn_threshold (int): The threshold for how long it will
        take to burn down a backlog before scaling up. Increasing this number
        will cause your consumer to scale up more aggresively. Defaults to 60.
    consumer_cpu_percent_target (int): The target cpu percentage for scaling
        down. Increasing this number will cause your consumer to scale down
        more aggresively. Defaults to 25.
    """

    enable_autoscaler: bool
    num_replicas: int
    min_replicas: int
    max_replicas: int
    autoscale_frequency_secs: int = 60
    # Options for configuring scaling for consumers
    consumer_backlog_burn_threshold: int = 60
    consumer_cpu_percent_target: int = 25
    # Options for configuring scaling for collectors and endpoints
    target_num_ongoing_requests_per_replica: int = 1
    max_concurrent_queries: int = 100

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
            self.consumer_cpu_percent_target < 0
            or self.consumer_cpu_percent_target > 100
        ):
            raise ValueError("consumer_cpu_percent_target must be between 0 and 100")
        if self.min_replicas < 0:
            raise ValueError("min_replicas must be greater than 0")
        if self.max_replicas < 0:
            raise ValueError("max_replicas must be greater than 0")
        if self.max_replicas < self.min_replicas:
            raise ValueError(
                "max_replicas must be greater than or equal to min_replicas"
            )
        if self.num_replicas < self.min_replicas:
            raise ValueError(
                "num_replicas must be greater than or equal to min_replicas"
            )


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
    # misc
    log_level: str
    checkin_frequency_loop_secs: int = 5

    @classmethod
    def default(cls) -> "RuntimeOptions":
        return cls(
            processor_options={},
            log_level="INFO",
        )
