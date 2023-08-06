from typing import Optional

from buildflow.core.options._options import Options
from buildflow.core.options.credentials_options import (
    AWSCredentialsOptions,
    CredentialsOptions,
    GCPCredentialsOptions,
)
from buildflow.core.options.infra_options import InfraOptions, PulumiOptions
from buildflow.core.options.runtime_options import AutoscalerOptions, RuntimeOptions


class FlowOptions(Options):
    def __init__(
        self,
        *,
        # Runtime options
        num_replicas: int = 1,
        runtime_log_level: str = "INFO",
        # Credential Options
        gcp_service_account_info: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        # Autoscaler options
        enable_autoscaler: bool = True,
        min_replicas: int = 1,
        max_replicas: int = 1000,
        pipeline_backlog_burn_threshold: int = 60,
        pipeline_cpu_percent_target: int = 25,
        # Infra options
        schema_validation: str = "none",
        require_confirmation: bool = True,
        infra_log_level: str = "INFO",
        # Pulumi options
        enable_destroy_protection: bool = False,
        refresh_state: bool = True,
    ) -> None:
        """Options for configuring a Flow.

        Args:
            num_replicas (int): The number of replicas to start with. Defaults to 1.
            runtime_log_level (str): The log level for the runtime. Defaults to "INFO".
            gcp_service_account_info: JSON string containing the service account info.
                Can either be a service account key, or JSON config for workflow
                identity federation.
            enable_autoscaler (bool): Whether or not autoscaling should be enabled.
                Defaults to True.
            min_replicas (int): The minimum number of replicas to scale down to.
                Defaults to 1.
            max_replicas (int): The maximum number of replicas to scale up to.
                Defaults to 1000.
            pipeline_backlog_burn_threshold (int): The threshold for how long it will
                take to burn down a backlog before scaling up. Increasing this number
                will cause your pipeline to scale up more aggresively. Defaults to 60.
            pipeline_cpu_percent_target (int): The target cpu percentage for scaling
                down. Increasing this number will cause your pipeline to scale down
                more aggresively. Defaults to 25.
            schema_validation (str): Whether schema validation should be enabled.
                Valid values are: strict, warning, none. Defaults to "none".
            require_confirmation (bool): Whether or not confirmation should be
                required before applying changes. Defaults to True.
            infra_log_level (str): The log level for the infra. Defaults to "INFO".
            enable_destroy_protection (bool): Whether destroy projection should be
                enabled for pulumi. Defaults to False.
            refresh_state (bool): Whether or not to refresh state before applying
                changes. Defaults to True.
        """
        super().__init__()
        self.num_replicas = num_replicas
        self.runtime_log_level = runtime_log_level
        self.enable_autoscaler = enable_autoscaler
        self.min_replicas = min_replicas
        self.max_replicas = max_replicas
        self.pipeline_backlog_burn_threshold = pipeline_backlog_burn_threshold
        self.pipeline_cpu_percent_target = pipeline_cpu_percent_target
        self.schema_validation = schema_validation
        self.require_confirmation = require_confirmation
        self.infra_log_level = infra_log_level
        self.enable_destroy_protection = enable_destroy_protection
        self.refresh_state = refresh_state
        self.runtime_options = RuntimeOptions(
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
            ),
            schema_validation=self.schema_validation,
            require_confirmation=self.require_confirmation,
            log_level=self.infra_log_level,
        )

    @classmethod
    def default(cls) -> Options:
        return FlowOptions()
