import dataclasses

from buildflow.core.options._options import Options
from buildflow.core.options.infra_options import InfraOptions
from buildflow.core.options.runtime_options import RuntimeOptions
from buildflow.core.options.resource_options import ResourceOptions


@dataclasses.dataclass
class FlowOptions(Options):
    runtime_options: RuntimeOptions
    infra_options: InfraOptions
    resource_options: ResourceOptions

    @classmethod
    def default(cls) -> "FlowOptions":
        return cls(
            runtime_options=RuntimeOptions.default(),
            infra_options=InfraOptions.default(),
            resource_options=ResourceOptions.default(),
        )
