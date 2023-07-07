from dataclasses import is_dataclass
from typing import Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.io.primitives.gcp.strategies.bigquery_strategies import (
    StreamingBigQueryTableSink,
)
from buildflow.core.io.schemas import bigquery_schemas
from buildflow.core.providers.provider import (
    PulumiProvider,
    SinkProvider,
)
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.types.gcp_types import (
    DatasetName,
    ProjectID,
    TableID,
    TableName,
)


class BigQueryTableProvider(SinkProvider, PulumiProvider):
    def __init__(
        self,
        *,
        project_id: ProjectID,
        dataset_name: DatasetName,
        table_name: TableName,
        # sink-only options
        batch_size: str = 10_000,
        # pulumi-only options
        include_dataset: bool = True,
        destroy_protection: bool = True,
    ):
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.table_name = table_name
        # sink-only options
        self.batch_size = batch_size
        # pulumi-only options
        self.include_dataset = include_dataset
        self.destroy_protection = destroy_protection

    @property
    def table_id(self) -> TableID:
        return f"{self.project_id}.{self.dataset_name}.{self.table_name}"

    def sink(self):
        return StreamingBigQueryTableSink(
            project_id=self.project_id,
            dataset_name=self.dataset_name,
            table_name=self.table_name,
            batch_size=self.batch_size,
        )

    def pulumi_resources(self, type_: Optional[Type]):
        resources = []

        if self.include_dataset:
            dataset_resource_id = f"{self.project_id}.{self.dataset_name}"
            dataset_resource = pulumi_gcp.bigquery.Dataset(
                resource_name=dataset_resource_id,
                project=self.project_id,
                dataset_id=self.dataset_name,
            )
            pulumi.export("gcp.bigquery.dataset_id", dataset_resource.dataset_id)
            resources.append(
                PulumiResource(
                    resource_id=dataset_resource_id,
                    resource=dataset_resource,
                    exports={
                        "gcp.bigquery.dataset_id": dataset_resource.dataset_id,
                    },
                )
            )

        schema = None
        if hasattr(type_, "__args__"):
            # Using a composite type hint like List or Optional
            type_ = type_.__args__[0]
        if type_ and is_dataclass(type_):
            schema = bigquery_schemas.dataclass_to_json_bq_schema(type_)

        parent = dataset_resource if self.include_dataset else None
        table_resource_id = self.table_id
        table_resource = pulumi_gcp.bigquery.Table(
            resource_name=table_resource_id,
            project=self.project_id,
            dataset_id=self.dataset_name,
            table_id=self.table_name,
            schema=schema,
            deletion_protection=(
                self.destroy_protection if not self.destroy_protection else None
            ),
            opts=pulumi.ResourceOptions(parent=parent),
        )
        pulumi.export("gcp.bigquery.table_id", table_resource.table_id)
        resources.append(
            PulumiResource(
                resource_id=table_resource_id,
                resource=table_resource,
                exports={
                    "gcp.bigquery.table_id": table_resource.table_id,
                },
            )
        )

        return resources
