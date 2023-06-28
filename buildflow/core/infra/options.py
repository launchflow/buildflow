import dataclasses
import enum

from buildflow.api.shared import Options


class SchemaValidation(enum.Enum):
    STRICT = "strict"
    WARNING = "warning"
    NONE = "none"


@dataclasses.dataclass
class PulumiOptions(Options):
    enable_destroy_protection: bool
    refresh_state: bool
    log_level: str

    @classmethod
    def default(cls):
        return cls(
            enable_destroy_protection=False, refresh_state=False, log_level="INFO"
        )


@dataclasses.dataclass
class InfraOptions(Options):
    pulumi_options: PulumiOptions
    # TODO: enforce schema validation
    schema_validation: SchemaValidation
    require_confirmation: bool
    log_level: str

    @classmethod
    def default(cls):
        return cls(
            pulumi_options=PulumiOptions.default(),
            schema_validation=SchemaValidation.WARNING,
            require_confirmation=False,
            log_level="INFO",
        )
