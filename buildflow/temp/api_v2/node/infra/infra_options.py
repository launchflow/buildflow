import enum

from buildflow.api_v2.node.options import OptionsAPI


class SchemaValidation(enum.Enum):
    STRICT = "strict"
    WARNING = "warning"
    NONE = "none"


class PulumiOptions(OptionsAPI):
    enable_destroy_protection: bool
    refresh_state: bool
    log_level: str


class InfraOptions(OptionsAPI):
    pulumi_options: PulumiOptions
    schema_validation: SchemaValidation
    require_confirmation: bool
    log_level: str
