# ruff: noqa: E501
from typing import Dict

from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.options import (
    Options,
)
from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.patterns.processor import (
    ProcessorID,
)


class ReplicaOptions(Options):
    num_cpus: float
    num_concurrency: int
    log_level: str


class AutoscalerOptions(Options):
    enable_autoscaler: bool
    min_replicas: int
    max_replicas: int
    log_level: str


class RuntimeOptions(Options):
    # the configuration of each replica
    replica_options: Dict[ProcessorID, ReplicaOptions]
    # the configuration of the autoscaler
    autoscaler_options: AutoscalerOptions
    # the number of replicas to start with (will scale up/down if autoscale is enabled)
    num_replicas: int
    # misc
    log_level: str
