import enum
from typing import Union

from buildflow.api.io import SinkType, SourceType
from buildflow.core import utils
from buildflow.resources.config import (
    AWSResourceConfig,
    AzureResourceConfig,
    GCPResourceConfig,
    LocalResourceConfig,
    ResourcesConfig,
)
from buildflow.resources.io import (
    BigQueryTable,
    GCPPubSubSubscription,
    GCPPubSubTopic,
    GCSFileStream,
    ResourceType,
)


class ResourceProviderType(enum.Enum):
    LOCAL = "local"
    GCP = "gcp"
    AWS = "aws"
    AZURE = "azure"


# NOTE: This is the higher-level (declarative) API for defining resources.
# See the ResourceType class in the io submodule for the lower-level API.
class MetaResourceType(ResourceType):
    def __init__(
        self, resource_id: str, resource_provider: Union[ResourceProviderType, str]
    ):
        # converts the provider type string to the enum if needed
        if isinstance(resource_provider, str):
            resource_provider = ResourceProviderType(resource_provider)

        self.resource_id = resource_id
        self.resource_provider = resource_provider

    # TODO: Is there a better way to do this? Feels hacky.
    # NOTE: This method is added to the ResourceType class at runtime by the Node API.
    def get_resource_config(self) -> ResourcesConfig:
        raise NotImplementedError("get_resource_config not implemented")

    # TODO: Is there a better way to do this? Feels hacky.
    # NOTE: This method is added to the ResourceType class at runtime by the Node API.
    def get_io_type(self) -> Union[SourceType, SinkType]:
        raise NotImplementedError("get_io_type not implemented")

    # This method wraps the lower-level provider() method and lets us inject the
    # resource config and io type at runtime.
    def provider(self):
        return self.resource_type().provider()

    def resource_type(self) -> ResourceType:
        resource_config = self.get_resource_config()
        io_type = self.get_io_type()

        if self.resource_provider == ResourceProviderType.LOCAL:
            self.local_resource_type(io_type, resource_config.local)
        elif self.resource_provider == ResourceProviderType.GCP:
            return self.gcp_resource_type(io_type, resource_config.gcp)
        elif self.resource_provider == ResourceProviderType.AWS:
            return self.aws_resource_type(io_type, resource_config.aws)
        elif self.resource_provider == ResourceProviderType.AZURE:
            return self.azure_resource_type(io_type, resource_config.azure)
        else:
            raise ValueError(
                f"Invalid resource provider type: {self.resource_provider}"
            )

    def local_resource_type(
        self, io_type: Union[SourceType, SinkType], local_config: LocalResourceConfig
    ) -> ResourceType:
        class_name = self.__class__.__name__
        raise NotImplementedError(
            f"{class_name}.local_resource_type not implemented for {io_type}"
        )

    def gcp_resource_type(
        self, io_type: Union[SourceType, SinkType], gcp_config: GCPResourceConfig
    ) -> ResourceType:
        class_name = self.__class__.__name__
        raise NotImplementedError(
            f"{class_name}.gcp_resource_type not implemented for {io_type}"
        )

    def aws_resource_type(
        self, io_type: Union[SourceType, SinkType], aws_config: AWSResourceConfig
    ) -> ResourceType:
        class_name = self.__class__.__name__
        raise NotImplementedError(
            f"{class_name}.aws_resource_type not implemented for {io_type}"
        )

    def azure_resource_type(
        self, io_type: Union[SourceType, SinkType], azure_config: AzureResourceConfig
    ) -> ResourceType:
        class_name = self.__class__.__name__
        raise NotImplementedError(
            f"{class_name}.azure_resource_type not implemented for {io_type}"
        )


class Topic(MetaResourceType):
    def __init__(
        self, resource_id: str, resource_provider: Union[ResourceProviderType, str]
    ):
        super().__init__(resource_id, resource_provider)

    def gcp_resource_type(
        self, io_type: Union[SourceType, SinkType], gcp_config: GCPResourceConfig
    ) -> Union[GCPPubSubSubscription, GCPPubSubTopic]:
        project_id = gcp_config.default_project_id
        project_hash = utils.stable_hash(project_id)
        topic_name = f"buildflow_topic_{project_hash[:8]}"
        topic_id = f"projects/{project_id}/topics/{topic_name}"
        topic_hash = utils.stable_hash(topic_id)
        subscription_name = f"buildflow_subscription_{topic_hash[:8]}"

        if io_type == SourceType:
            return GCPPubSubSubscription(
                project_id=project_id,
                topic_id=topic_id,
                subscription_name=subscription_name,
            )
        elif io_type == SinkType:
            return GCPPubSubTopic(
                project_id=project_id,
                topic_name=topic_name,
            )

        raise NotImplementedError(
            f"Topic.gcp_resource_type not implemented for {io_type}"
        )


class Queue(MetaResourceType):
    def __init__(
        self, resource_id: str, resource_provider: Union[ResourceProviderType, str]
    ):
        super().__init__(resource_id, resource_provider)


class DataWarehouseTable(MetaResourceType):
    def __init__(
        self, resource_id: str, resource_provider: Union[ResourceProviderType, str]
    ):
        super().__init__(resource_id, resource_provider)

    def gcp_resource_type(
        self, io_type: Union[SourceType, SinkType], gcp_config: GCPResourceConfig
    ) -> BigQueryTable:
        project_id = gcp_config.default_project_id
        project_hash = utils.stable_hash(project_id)
        table_id = f"{project_id}.buildflow.table_{project_hash[:8]}"

        # source & sink are the same for bigquery
        if io_type == SourceType or io_type == SinkType:
            return BigQueryTable(table_id=table_id)

        raise NotImplementedError(
            f"DataWarehouseTable.gcp_resource_type not implemented for {io_type}"
        )


class RelationalTable(MetaResourceType):
    def __init__(
        self, resource_id: str, resource_provider: Union[ResourceProviderType, str]
    ):
        super().__init__(resource_id, resource_provider)


class CloudStorage(MetaResourceType):
    def __init__(
        self, resource_id: str, resource_provider: Union[ResourceProviderType, str]
    ):
        super().__init__(resource_id, resource_provider)


class FileStream(MetaResourceType):
    def __init__(
        self, resource_id: str, resource_provider: Union[ResourceProviderType, str]
    ):
        super().__init__(resource_id, resource_provider)

    def gcp_resource_type(
        self, io_type: SourceType, gcp_config: GCPResourceConfig
    ) -> GCSFileStream:
        project_id = gcp_config.default_project_id
        project_hash = utils.stable_hash(project_id)
        bucket_name = f"buildflow_bucket_{project_hash[:8]}"

        # FileStream can only be used as a source
        if io_type == SourceType:
            return GCSFileStream(project_id=project_id, bucket_name=bucket_name)

        raise NotImplementedError(
            f"FileStream.gcp_resource_type not implemented for {io_type}"
        )
