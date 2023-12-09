from typing import Optional

from buildflow.core.options._options import Options
from buildflow.core.options.credentials_options import (
    AWSCredentialsOptions,
    CredentialsOptions,
    GCPCredentialsOptions,
)
from buildflow.core.options.infra_options import InfraOptions, PulumiOptions
from buildflow.core.options.runtime_options import RuntimeOptions


class FlowOptions(Options):
    def __init__(
        self,
        *,
        # Runtime options
        runtime_log_level: str = "INFO",
        # Credential Options
        gcp_service_account_info: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        # Infra options
        schema_validation: str = "none",
        require_confirmation: bool = True,
        infra_log_level: str = "INFO",
        # Pulumi options
        stack: Optional[str] = None,
        enable_destroy_protection: bool = False,
        refresh_state: bool = True,
    ) -> None:
        """Options for configuring a Flow.

        Args:
            runtime_log_level (str): The log level for the runtime. Defaults to "INFO".
            gcp_service_account_info: JSON string containing the service account info.
                Can either be a service account key, or JSON config for workflow
                identity federation.
            schema_validation (str): Whether schema validation should be enabled.
                Valid values are: strict, warning, none. Defaults to "none".
            require_confirmation (bool): Whether or not confirmation should be
                required before applying changes. Defaults to True.
            infra_log_level (str): The log level for the infra. Defaults to "INFO".
            stack (str): The stack to use. Defaults to the default_stack field
                configured in your buildflow.yaml.
            enable_destroy_protection (bool): Whether destroy projection should be
                enabled for pulumi. Defaults to False.
            refresh_state (bool): Whether or not to refresh state before applying
                changes. Defaults to True.
        """
        super().__init__()
        self.runtime_log_level = runtime_log_level
        self.schema_validation = schema_validation
        self.require_confirmation = require_confirmation
        self.infra_log_level = infra_log_level
        self.enable_destroy_protection = enable_destroy_protection
        self.refresh_state = refresh_state
        self.runtime_options = RuntimeOptions(
            processor_options={},
            log_level=self.runtime_log_level,
        )
        self.credentials_options = CredentialsOptions(
            gcp_credentials_options=GCPCredentialsOptions(
                service_account_info=gcp_service_account_info
            ),
            aws_credentials_options=AWSCredentialsOptions(
                access_key_id=aws_access_key_id,
                secret_access_key=aws_secret_access_key,
                session_token=aws_session_token,
            ),
        )
        self.infra_options = InfraOptions(
            pulumi_options=PulumiOptions(
                enable_destroy_protection=self.enable_destroy_protection,
                refresh_state=self.refresh_state,
                log_level=self.infra_log_level,
                selected_stack=stack,
            ),
            schema_validation=self.schema_validation,
            require_confirmation=self.require_confirmation,
            log_level=self.infra_log_level,
        )

    @classmethod
    def default(cls) -> Options:
        return FlowOptions()
