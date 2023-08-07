from dataclasses import is_dataclass
from typing import Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.io.gcp.strategies.bigquery_strategies import (
    StreamingBigQueryTableSink,
)
from buildflow.core.io.utils.schemas import bigquery_schemas
from buildflow.core.providers.provider import PulumiProvider, SinkProvider
from buildflow.core.types.gcp_types import (
    BigQueryDatasetName,
    BigQueryTableID,
    BigQueryTableName,
    GCPProjectID,
)


class _BigQueryTablePulumiResource(pulumi.ComponentResource):
    def __init__(
        self,
        table_name: BigQueryTableName,
        dataset_name: BigQueryDatasetName,
        project_id: GCPProjectID,
        include_dataset: bool,
        destroy_protection: bool,
        schema: Optional[str],
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:gcp:bigquery:Table",
            f"buildflow-{project_id}-{dataset_name}-{table_name}",
            None,
            opts,
        )

        outputs = {}
        table_depends_on = None
        if include_dataset:
            self.dataset_resource = pulumi_gcp.bigquery.Dataset(
                f"buildflow-{dataset_name}",
                project=project_id,
                dataset_id=dataset_name,
                delete_contents_on_destroy=(not destroy_protection),
                opts=pulumi.ResourceOptions(parent=self),
            )
            outputs["gcp.bigquery.dataset_id"] = self.dataset_resource.id
            table_depends_on = [self.dataset_resource]

        self.table_resource = pulumi_gcp.bigquery.Table(
            f"buildflow-{table_name}",
            project=project_id,
            dataset_id=dataset_name,
            table_id=table_name,
            schema=schema,
            deletion_protection=destroy_protection,
            opts=pulumi.ResourceOptions(parent=self, depends_on=table_depends_on),
        )
        outputs["gcp.bigquery.table_id"] = self.table_resource.id

        self.register_outputs(outputs)


class BigQueryTableProvider(SinkProvider, PulumiProvider):
    def __init__(
        self,
        *,
        project_id: GCPProjectID,
        dataset_name: BigQueryDatasetName,
        table_name: BigQueryTableName,
        # sink-only options
        batch_size: str,
        # pulumi-only options
        include_dataset: bool,
        destroy_protection: bool,
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
    def table_id(self) -> BigQueryTableID:
        return f"{self.project_id}.{self.dataset_name}.{self.table_name}"

    def sink(self, credentials: GCPCredentials):
        return StreamingBigQueryTableSink(
            credentials=credentials,
            project_id=self.project_id,
            dataset_name=self.dataset_name,
            table_name=self.table_name,
            batch_size=self.batch_size,
        )

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: Optional[pulumi.ResourceOptions] = None,
    ):
        # TODO: Maybe throw an error if schema is None
        schema = None
        if hasattr(type_, "__args__"):
            # Using a composite type hint like List or Optional
            type_ = type_.__args__[0]
        if type_ and is_dataclass(type_):
            schema = bigquery_schemas.dataclass_to_json_bq_schema(type_)

        return _BigQueryTablePulumiResource(
            self.table_name,
            self.dataset_name,
            self.project_id,
            self.include_dataset,
            self.destroy_protection,
            schema,
            type_,
            credentials,
            opts,
        )
