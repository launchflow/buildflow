import dataclasses

from buildflow.core.options._options import Options
from buildflow.core.options.infra_options import InfraOptions, PulumiOptions
from buildflow.core.options.runtime_options import AutoscalerOptions, RuntimeOptions


@dataclasses.dataclass
class FlowOptions(Options):
    # Runtime options:
    #   The number of replicas to start with
    num_replicas: int = 1
    #   Log level for runtime
    runtime_log_level: str = "INFO"
    #   Whether or not autoscaling shoule be enabled.
    enable_autoscaler: bool = True
    #   Minimum number of replicas to scale down to.
    min_replicas: int = 1
    #   Maximum number of replicas to scale up to.
    max_replicas: int = 1000
    #   The threshold for how long it will take to burn down a
    #   backlog before scaleing up. Increasing this number
    #   will cause your pipeline to scale up more aggresively.
    pipeline_backlog_burn_threshold: int = 60
    #   The target cpu percentage for scaling down. Increasing
    #   this number will cause your pipeline to scale down more
    #   aggresively.
    pipeline_cpu_percent_target: int = 25
    # Infra options
    #   Whether schema validation should be enabled.
    #   Valid values are: strict, warning, none
    schema_validation: str = "none"
    #   Whether or not confirmation should be required before applying changes.
    require_confirmation: bool = True
    #   Whether destroy projection should be enabled for pulumi.
    enable_destroy_protection: bool = False
    #   Whether or not to refresh state before applying changes.
    refresh_state: bool = True
    #   Log level for infra
    infra_log_level: str = "INFO"

    def __post_init__(self):
        self._runtime_options = RuntimeOptions(
            processor_options={},
            num_replicas=self.num_replicas,
            log_level=self.runtime_log_level,
            autoscaler_options=AutoscalerOptions(
                enable_autoscaler=self.enable_autoscaler,
                min_replicas=self.min_replicas,
                max_replicas=self.max_replicas,
                log_level=self.runtime_log_level,
                pipeline_backlog_burn_threshold=self.pipeline_backlog_burn_threshold,
                pipeline_cpu_percent_target=self.pipeline_cpu_percent_target,
            ),
        )
        self._infra_options = InfraOptions(
            pulumi_options=PulumiOptions(
                enable_destroy_protection=self.enable_destroy_protection,
                refresh_state=self.refresh_state,
                log_level=self.infra_log_level,
            ),
            schema_validation=self.schema_validation,
            require_confirmation=self.require_confirmation,
            log_level=self.infra_log_level,
        )

    @property
    def runtime_options(self) -> RuntimeOptions:
        return self._runtime_options

    @property
    def infra_options(self) -> InfraOptions:
        return self._infra_options

    @classmethod
    def default(cls) -> Options:
        return FlowOptions()
