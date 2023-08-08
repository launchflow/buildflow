import copy
import enum
from typing import Generic, Optional, final

from buildflow.config.cloud_provider_config import (
    AWSOptions,
    AzureOptions,
    CloudProviderConfig,
    GCPOptions,
    LocalOptions,
)
from buildflow.io.provider import BTT, PPT, SIT, SOT
from buildflow.io.strategies._strategy import StategyType


class PrimitiveType(enum.Enum):
    PORTABLE = "portable"
    GCP = "gcp"
    AWS = "aws"
    AZURE = "azure"
    LOCAL = "local"
    EMPTY = "empty"
    AGNOSTIC = "agnostic"


class Primitive(Generic[PPT, SOT, SIT, BTT]):
    primitive_type: PrimitiveType
    _managed: bool = False

    def enable_managed(self):
        """Enable managed mode."""
        self._managed = True

    def options(self, managed: bool = False) -> "Primitive":
        """Return a copy of this primitive with the managed flag set."""
        to_ret = copy.deepcopy(self)
        to_ret._managed = managed
        return to_ret

    @final
    def pulumi_provider(self) -> Optional[PPT]:
        if self._managed:
            return self._pulumi_provider()
        return None

    # NOTE: This is the method that users should implement for custom providers.
    def _pulumi_provider(self) -> PPT:
        """Return a pulumi provider for this primitive."""
        raise NotImplementedError(
            f"Primitive._pulumi_provider() is not implemented for type: {type(self)}."
        )

    def source_provider(self) -> SOT:
        """Return a source provider for this primitive."""
        raise NotImplementedError(
            f"Primitive.source_provider() is not implemented for type: {type(self)}."
        )

    def sink_provider(self) -> SIT:
        """Return a sink provider for this primitive."""
        raise NotImplementedError(
            f"Primitive.sink_provider() is not implemented for type: {type(self)}."
        )

    def background_task_provider(self) -> Optional[BTT]:
        """Return a background task provider for this primitive."""
        return None


class PortablePrimtive(Primitive):
    primitive_type = PrimitiveType.PORTABLE
    # Portable primitives are always managed.
    _managed: bool = True

    def to_cloud_primitive(
        self, cloud_provider_config: CloudProviderConfig, strategy_type: StategyType
    ) -> "Primitive":
        """Create a cloud primitive from a CloudProviderConfig."""
        raise NotImplementedError(
            "PortablePrimtive.to_cloud_primitive() is not implemented."
        )

    # Override the super class options method since it doesn't make sense to non-manage
    # a portable primitive.
    def options(self) -> Primitive:
        return copy.deepcopy(self)


class CompositePrimitive(Primitive):
    # Composite primitives are always managed.
    # They defer to their underlying primitives on what should actually be managed.
    _managed: bool = True

    def options(self) -> "Primitive":
        return copy.deepcopy(self)


class GCPPrimtive(Primitive[PPT, SOT, SIT, BTT]):
    # TODO: We need to check the infra State to warn the user if the infra has not been
    # created yet.
    primitive_type = PrimitiveType.GCP

    @classmethod
    def from_gcp_options(cls, gcp_options: GCPOptions) -> "GCPPrimtive":
        """Create a primitive from GCPOptions."""
        raise NotImplementedError("GCPPrimtive.from_gcp_options() is not implemented.")


class AWSPrimtive(Primitive[PPT, SOT, SIT, BTT]):
    primitive_type = PrimitiveType.AWS

    @classmethod
    def from_aws_options(cls, aws_options: AWSOptions) -> "AWSPrimtive":
        """Create a primitive from AWSOptions."""
        raise NotImplementedError("AWSPrimtive.from_aws_options() is not implemented.")


class AzurePrimtive(Primitive[PPT, SOT, SIT, BTT]):
    primitive_type = PrimitiveType.AZURE

    @classmethod
    def from_azure_options(cls, azure_options: AzureOptions) -> "AzurePrimtive":
        """Create a primitive from AzureOptions."""
        raise NotImplementedError(
            "AzurePrimtive.from_azure_options() is not implemented."
        )


class LocalPrimtive(Primitive[PPT, SOT, SIT, BTT]):
    primitive_type = PrimitiveType.LOCAL
    # LocalPrimitives are never managed.
    managed: bool = False

    @classmethod
    def from_local_options(cls, local_options: LocalOptions) -> "LocalPrimtive":
        """Create a primitive from LocalOptions."""
        raise NotImplementedError(
            "LocalPrimtive.from_local_options() is not implemented."
        )

    # Composite primitives are always managed.
    # They defer to their underlying primitives on what should actually be managed.
    def options(self) -> "Primitive":
        return copy.deepcopy(self)
