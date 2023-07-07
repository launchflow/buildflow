from typing import Dict

from buildflow.api_v2.node.options import OptionsAPI
from buildflow.api_v2.node.pattern._patterns.processor import ProcessorID


# TODO: Add options for other pattern types, or merge into a single options object
class ProcessorOptions(OptionsAPI):
    num_cpus: float
    num_concurrency: int
    log_level: str


class AutoscalerOptions(OptionsAPI):
    enable_autoscaler: bool
    min_replicas: int
    max_replicas: int
    log_level: str


class RuntimeOptions(OptionsAPI):
    # the configuration of each processor
    processor_options: Dict[ProcessorID, ProcessorOptions]
    # the configuration of the autoscaler
    autoscaler_options: AutoscalerOptions
    # the number of replicas to start with (will scale up/down if autoscale is enabled)
    num_replicas: int
    # misc
    log_level: str
