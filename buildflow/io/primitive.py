import enum
from typing import Any, Callable, Dict, List, Optional, Type

import pulumi
from starlette.requests import Request

from buildflow.config.cloud_provider_config import (
    AWSOptions,
    AzureOptions,
    CloudProviderConfig,
    GCPOptions,
    LocalOptions,
)
from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials import CredentialType
from buildflow.dependencies.base import NoScoped
from buildflow.io.strategies._strategy import StategyType
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.strategies.source import SourceStrategy


class PrimitiveType(enum.Enum):
    PORTABLE = "portable"
    GCP = "gcp"
    AWS = "aws"
    AZURE = "azure"
    LOCAL = "local"
    EMPTY = "empty"
    AGNOSTIC = "agnostic"


class Primitive:
    primitive_type: PrimitiveType
    _managed: bool = False

    def primitive_id(self):
        raise NotImplementedError(
            f"Primitive.primitive_id() is not implemented for type: {type(self)}."
        )

    def enable_managed(self):
        self._managed = True

    def options(self) -> "Primitive":
        return self

    def pulumi_resources_if_managed(
        self,
        credentials: CredentialType,
        opts: pulumi.ResourceOptions,
    ):
        if not self._managed:
            return None
        return self.pulumi_resources(credentials, opts)

    def pulumi_resources(
        self,
        credentials: CredentialType,
        opts: pulumi.ResourceOptions,
    ) -> List[pulumi.Resource]:
        raise NotImplementedError(
            f"Primitive.pulumi_resources() is not implemented for type: {type(self)}."
        )

    def source(self, credentials: CredentialType) -> SourceStrategy:
        raise NotImplementedError(
            f"Primitive.source() is not implemented for type: {type(self)}."
        )

    def sink(self, credentials: CredentialType) -> SinkStrategy:
        raise NotImplementedError(
            f"Primitive.sink() is not implemented for type: {type(self)}."
        )

    def background_tasks(self, credentials: CredentialType) -> List[BackgroundTask]:
        return []

    def dependency(self) -> "PrimitiveDependency":
        """Returns a dependency allowing the primitive to be injected as a dependency.

        For example:

        prim = Primitive(...)
        PrimDep = prim.dependency()

        @dependency(scope=Scope.PROCESS)
        class MyDep:
        def __init__(self, injected_primitive: PrimDep):
            self.prim_field = injected_primitive.field_on_primitive
        """
        return PrimitiveDependency(self)

    def cloud_console_url(self) -> Optional[str]:
        """Returns a URL to the cloud console for this primitive."""
        return None


# Dependency that wraps the primitive that allows the primitive to be injected as a
# dependency.
class PrimitiveDependency(NoScoped):
    def __init__(self, primitive: Primitive):
        super().__init__(lambda: primitive)
        self.primitive = primitive

    async def resolve(
        self,
        flow_dependencies: Dict[Type, Any],
        visited_dependencies: Dict[Callable, Any],
        request: Optional[Request] = None,
    ):
        return self.primitive


class PortablePrimtive(Primitive):
    primitive_type = PrimitiveType.PORTABLE

    def to_cloud_primitive(
        self, cloud_provider_config: CloudProviderConfig, strategy_type: StategyType
    ) -> "Primitive":
        """Create a cloud primitive from a CloudProviderConfig."""
        raise NotImplementedError(
            "PortablePrimtive.to_cloud_primitive() is not implemented."
        )


class GCPPrimtive(Primitive):
    # TODO: We need to check the infra State to warn the user if the infra has not been
    # created yet.
    primitive_type = PrimitiveType.GCP

    @classmethod
    def from_gcp_options(cls, gcp_options: GCPOptions) -> "GCPPrimtive":
        """Create a primitive from GCPOptions."""
        raise NotImplementedError("GCPPrimtive.from_gcp_options() is not implemented.")


class AWSPrimtive(Primitive):
    primitive_type = PrimitiveType.AWS

    @classmethod
    def from_aws_options(cls, aws_options: AWSOptions) -> "AWSPrimtive":
        """Create a primitive from AWSOptions."""
        raise NotImplementedError("AWSPrimtive.from_aws_options() is not implemented.")


class AzurePrimtive(Primitive):
    primitive_type = PrimitiveType.AZURE

    @classmethod
    def from_azure_options(cls, azure_options: AzureOptions) -> "AzurePrimtive":
        """Create a primitive from AzureOptions."""
        raise NotImplementedError(
            "AzurePrimtive.from_azure_options() is not implemented."
        )


class LocalPrimtive(Primitive):
    primitive_type = PrimitiveType.LOCAL

    @classmethod
    def from_local_options(cls, local_options: LocalOptions) -> "LocalPrimtive":
        """Create a primitive from LocalOptions."""
        raise NotImplementedError(
            "LocalPrimtive.from_local_options() is not implemented."
        )
