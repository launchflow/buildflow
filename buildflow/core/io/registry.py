from typing import Callable, Dict, Type

from buildflow.api.composites.sink import Sink
from buildflow.api.composites.source import Source
from buildflow.api.primitives._primitive import PrimitiveAPI
from buildflow.api.primitives.asynchronous.topic import Topic
from buildflow.api.primitives.db.table import AnalysisTable
from buildflow.api.primitives.empty import Empty
from buildflow.core.io.providers._provider import Provider
from buildflow.core.io.providers.empty import EmptyProvider
from buildflow.core.io.providers.gcp.bigquery import StreamingBigQueryProvider
from buildflow.core.io.providers.gcp.pubsub import (
    GCPPubSubSubscriptionProvider,
    GCPPubSubTopicProvider,
)
from buildflow.core.io.providers._provider import PulumiResources

from buildflow.core.io.resources._resource import Resource
from buildflow.core.io.resources.empty import EmptyResource
from buildflow.core.io.resources.gcp.bigquery import BigQueryTable
from buildflow.core.io.resources.gcp.pubsub import GCPPubSubSubscription, GCPPubSubTopic
from buildflow.core.options.resource_options import ResourceOptions, ResourceProvider
from buildflow.core.io.providers._provider import SinkProvider, SourceProvider


def _gcp_pubsub_subscription_constructor(subscription: GCPPubSubSubscription):
    return GCPPubSubSubscriptionProvider(
        project_id=subscription.project_id,
        topic_id=subscription.topic_id,
        subscription_name=subscription.subscription_name,
    )


def _gcp_pubsub_topic_constructor(topic: GCPPubSubTopic):
    return GCPPubSubTopicProvider(
        project_id=topic.project_id, topic_name=topic.topic_name
    )


def _bigquery_table_constructor(table: BigQueryTable):
    project_id, _, _ = table.table_id.split(".")
    return StreamingBigQueryProvider(
        project_id=project_id,
        table_id=table.table_id,
        destroy_protection=False,
        include_dataset=True,
    )


def _empty_constructor(empty: EmptyResource):
    return EmptyProvider()


class Registry:
    def __init__(self, resource_options: ResourceOptions):
        # Registry configuration
        self.options = resource_options
        # Registry initial state
        self._resource_provider_constructors: Dict[
            Type[Resource], Callable[[Resource], Provider]
        ] = {
            GCPPubSubSubscription: _gcp_pubsub_subscription_constructor,
            GCPPubSubTopic: _gcp_pubsub_topic_constructor,
            BigQueryTable: _bigquery_table_constructor,
            EmptyResource: _empty_constructor,
        }
        self._primitive_resource_types: Dict[
            Type[PrimitiveAPI], Dict[ResourceProvider, Type[Resource]]
        ] = {  # noqa: E501
            # TODO: Add other resource providers (aws, local, etc.)
            Topic: {
                ResourceProvider.GCP: GCPPubSubTopic,
            },
            AnalysisTable: {
                ResourceProvider.GCP: BigQueryTable,
            },
            Empty: {
                ResourceProvider.GCP: EmptyResource,
            },
        }

    def register_resource_provider_constructor(
        self, resource_type: Type[Resource], constructor: Callable[[Resource], Provider]
    ):
        self._resource_provider_constructors[resource_type] = constructor

    def register_primitive_resource_type(
        self,
        primitive_type: Type[PrimitiveAPI],
        resource_provider: ResourceProvider,
        resource_type: Type[Resource],
    ):
        if primitive_type not in self._primitive_resource_types:
            self._primitive_resource_types[primitive_type] = {}
        self._primitive_resource_types[primitive_type][
            resource_provider
        ] = resource_type

    def get_provider_constructor_for_resource_type(
        self, resource_type: Type[Resource]
    ) -> Callable[[Resource], Provider]:
        provider_constructor = self._resource_provider_constructors.get(
            resource_type, None
        )
        if provider_constructor is None:
            raise ValueError(f"Resource type {resource_type} not found in registry.")

        return provider_constructor

    def get_resource_for_primitive(self, primitive: PrimitiveAPI) -> Resource:
        resource_type = self._primitive_resource_types.get(type(primitive), {}).get(
            self.options.resource_provider, None
        )
        if resource_type is None:
            raise ValueError(
                f"Primitive {primitive} not found in registry for resource provider "
                f"{self.options.resource_provider}."
            )
        return resource_type.from_options(self.options)

    def get_source_provider_for_resource(self, resource: Resource) -> SourceProvider:
        provider_constructor = self.get_provider_constructor_for_resource_type(
            type(resource)
        )

        if resource.exclude_from_infra:

            def pulumi_resources_fn(self, type_):
                return PulumiResources([], {})

        else:

            def pulumi_resources_fn(self, type_):
                return provider_constructor(resource).pulumi_resources(type_)

        _AdHocSourceProvider = type(
            f"{type(resource).__name__}SourceProvider",
            (SourceProvider,),
            {
                "source": lambda self: provider_constructor(resource),
                "pulumi_resources": pulumi_resources_fn,
            },
        )

        return _AdHocSourceProvider()

    def get_sink_provider_for_resource(self, resource: Resource) -> SinkProvider:
        provider_constructor = self.get_provider_constructor_for_resource_type(
            type(resource)
        )

        if resource.exclude_from_infra:

            def pulumi_resources_fn(self, type_):
                return PulumiResources([], {})

        else:

            def pulumi_resources_fn(self, type_):
                return provider_constructor(resource).pulumi_resources(type_)

        _AdHocSinkProvider = type(
            f"{type(resource).__name__}SinkProvider",
            (SinkProvider,),
            {
                "sink": lambda self: provider_constructor(resource),
                "pulumi_resources": pulumi_resources_fn,
            },
        )

        return _AdHocSinkProvider()

    def get_source_provider_for_primitive(
        self, primitive: PrimitiveAPI
    ) -> SourceProvider:
        resource = self.get_resource_for_primitive(primitive)
        return self.get_source_provider_for_resource(resource)

    def get_sink_provider_for_primitive(self, primitive: PrimitiveAPI) -> SinkProvider:
        resource = self.get_resource_for_primitive(primitive)
        return self.get_sink_provider_for_resource(resource)
