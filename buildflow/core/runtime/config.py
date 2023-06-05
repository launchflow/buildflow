import dataclasses
from typing import Dict

from buildflow.api import ProcessorID
from collections import defaultdict


@dataclasses.dataclass
class ReplicaConfig:
    num_cpus: float
    num_concurrent_tasks: int
    # misc
    log_level: str = "INFO"

    @classmethod
    def DEBUG(cls):
        return cls(num_cpus=0.5, num_concurrent_tasks=1)

    @classmethod
    def IO_BOUND(cls):
        return cls(num_cpus=0.2, num_concurrent_tasks=4)

    @classmethod
    def CPU_BOUND(cls):
        return cls(num_cpus=1, num_concurrent_tasks=1)


@dataclasses.dataclass
class AutoscalerConfig:
    enable_autoscaler: bool
    min_replicas: int
    max_replicas: int
    # misc
    log_level: str = "INFO"

    @classmethod
    def DEBUG(cls):
        return cls(enable_autoscaler=False, min_replicas=1, max_replicas=1)


@dataclasses.dataclass
class RuntimeConfig:
    # the configuration of each replica
    replica_configs: Dict[ProcessorID, ReplicaConfig] = dataclasses.field(
        default_factory=lambda: defaultdict(ReplicaConfig.DEBUG)
    )
    # the configuration of the autoscaler
    autoscaler_config: AutoscalerConfig = AutoscalerConfig.DEBUG()
    # the number of replicas to start with (will scale up/down if autoscale is enabled)
    num_replicas: int = 1
    # misc
    log_level: str = "INFO"

    @classmethod
    def IO_BOUND(cls, autoscale=True):
        # defaults to the DEBUG autoscaler config
        autoscaler_config = AutoscalerConfig.DEBUG()
        if autoscale:
            autoscaler_config = AutoscalerConfig(
                enable_autoscaler=True,
                min_replicas=1,
                max_replicas=1000,
            )
        return cls(
            replica_configs=defaultdict(ReplicaConfig.IO_BOUND),
            autoscaler_config=autoscaler_config,
        )

    @classmethod
    def CPU_BOUND(cls, autoscale=True):
        # defaults to the DEBUG autoscaler config
        autoscaler_config = AutoscalerConfig.DEBUG()
        if autoscale:
            autoscaler_config = AutoscalerConfig(
                enable_autoscaler=True,
                min_replicas=1,
                max_replicas=1000,
            )
        return cls(
            replica_configs=defaultdict(ReplicaConfig.CPU_BOUND),
            autoscaler_config=autoscaler_config,
        )

    @classmethod
    def DEBUG(cls):
        return cls(
            replica_configs=defaultdict(ReplicaConfig.DEBUG),
            autoscaler_config=AutoscalerConfig.DEBUG(),
            log_level="DEBUG",
        )
