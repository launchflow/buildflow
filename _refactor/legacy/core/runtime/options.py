import dataclasses
from typing import Dict

from buildflow.api import ProcessorID
from buildflow.api.shared import Options


@dataclasses.dataclass
class ReplicaOptions(Options):
    num_cpus: float
    num_concurrency: int
    # misc (default values allowed)
    log_level: str = "INFO"

    @classmethod
    def default(cls):
        return cls(num_cpus=1, num_concurrency=1)

    @classmethod
    def DEBUG(cls):
        return cls(num_cpus=0.5, num_concurrency=1)

    @classmethod
    def IO_BOUND(cls):
        return cls(num_cpus=0.2, num_concurrency=4)

    @classmethod
    def CPU_BOUND(cls):
        return cls(num_cpus=1, num_concurrency=1)


@dataclasses.dataclass
class AutoscalerOptions(Options):
    enable_autoscaler: bool
    min_replicas: int
    max_replicas: int
    # misc (default values allowed)
    log_level: str = "INFO"

    @classmethod
    def default(cls):
        return cls(enable_autoscaler=False, min_replicas=1, max_replicas=1)

    @classmethod
    def DEBUG(cls):
        return cls(enable_autoscaler=False, min_replicas=1, max_replicas=1)


@dataclasses.dataclass
class RuntimeOptions(Options):
    # the configuration of each replica
    replica_options: Dict[ProcessorID, ReplicaOptions]
    # the configuration of the autoscaler
    autoscaler_options: AutoscalerOptions
    # the number of replicas to start with (will scale up/down if autoscale is enabled)
    num_replicas: int
    # misc
    log_level: str

    @classmethod
    def default(cls):
        return cls(
            replica_options={},
            autoscaler_options=AutoscalerOptions.default(),
            num_replicas=1,
            log_level="INFO",
        )
