# ruff: noqa: E501
import enum

from buildflow.api.workspaces.workspace.projects.project.deployments.deployment.node._shared.options import (
    Options,
)


class SchemaValidation(enum.Enum):
    STRICT = "strict"
    WARNING = "warning"
    NONE = "none"


class PulumiOptions(Options):
    enable_destroy_protection: bool
    refresh_state: bool
    log_level: str


class InfraOptions(Options):
    pulumi_options: PulumiOptions
    schema_validation: SchemaValidation
    require_confirmation: bool
    log_level: str
