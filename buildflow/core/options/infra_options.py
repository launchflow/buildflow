import dataclasses
import enum

from buildflow.core.options._options import Options


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
    def default(cls) -> "PulumiOptions":
        return cls(
            enable_destroy_protection=False,
            refresh_state=True,
            log_level="DEBUG",
        )


@dataclasses.dataclass
class InfraOptions(Options):
    pulumi_options: PulumiOptions
    schema_validation: SchemaValidation
    require_confirmation: bool
    log_level: str

    @classmethod
    def default(cls) -> "InfraOptions":
        return cls(
            pulumi_options=PulumiOptions.default(),
            schema_validation=SchemaValidation.NONE,
            require_confirmation=True,
            log_level="DEBUG",
        )
