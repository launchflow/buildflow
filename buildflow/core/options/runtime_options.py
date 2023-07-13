import dataclasses
from typing import Dict

from buildflow.core.options._options import Options
from buildflow.core.processor.processor import ProcessorID


# TODO: Add options for other pattern types, or merge into a single options object
@dataclasses.dataclass
class ProcessorOptions(Options):
    num_cpus: float
    num_concurrency: int
    log_level: str

    @classmethod
    def default(cls) -> "ProcessorOptions":
        return cls(
            num_cpus=1.0,
            num_concurrency=1,
            log_level="INFO",
        )


@dataclasses.dataclass
class AutoscalerOptions(Options):
    enable_autoscaler: bool
    min_replicas: int
    max_replicas: int
    log_level: str
    autoscale_frequency_secs: int = 60
    pipeline_backlog_burn_threshold: int = 60
    pipeline_cpu_percent_target: int = 25

    @classmethod
    def default(cls) -> "AutoscalerOptions":
        return cls(
            enable_autoscaler=True,
            min_replicas=1,
            max_replicas=1000,
            log_level="INFO",
        )


@dataclasses.dataclass
class RuntimeOptions(Options):
    # the configuration of each processor
    processor_options: Dict[ProcessorID, ProcessorOptions]
    # the configuration of the autoscaler
    autoscaler_options: AutoscalerOptions
    # the number of replicas to start with (will scale up/down if autoscale is enabled)
    num_replicas: int
    # misc
    log_level: str
    checkin_frequency_loop_secs: int = 5

    @classmethod
    def default(cls) -> "RuntimeOptions":
        return cls(
            processor_options={},
            autoscaler_options=AutoscalerOptions.default(),
            num_replicas=1,
            log_level="INFO",
        )
