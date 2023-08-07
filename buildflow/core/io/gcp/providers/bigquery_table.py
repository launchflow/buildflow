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


class BigQueryTableProvider(SinkProvider, PulumiProvider):
    def __init__(
        self,
        *,
        project_id: GCPProjectID,
        dataset_name: BigQueryDatasetName,
        table_name: BigQueryTableName,
        # sink-only options
        batch_size: str = 10_000,
        # pulumi-only options
        include_dataset: bool = True,
        destroy_protection: bool = False,
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

    def pulumi(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
    ):
        schema = None
        if hasattr(type_, "__args__"):
            # Using a composite type hint like List or Optional
            type_ = type_.__args__[0]
        if type_ and is_dataclass(type_):
            schema = bigquery_schemas.dataclass_to_json_bq_schema(type_)

        class BigQueryTable(pulumi.ComponentResource):
            def __init__(
                self,
                table_name: BigQueryTableName,
                dataset_name: BigQueryDatasetName,
                project_id: GCPProjectID,
                include_dataset: bool,
                destroy_protection: bool,
            ):
                component_name = f"buildflow-component-{table_name}"
                props: pulumi.Inputs | None = None
                opts: pulumi.ResourceOptions | None = None
                super().__init__(
                    "buildflow:gcp:bigquery:Table", component_name, props, opts
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
                    opts=pulumi.ResourceOptions(
                        parent=self, depends_on=table_depends_on
                    ),
                )
                outputs["gcp.bigquery.table_id"] = self.table_resource.id

                self.register_outputs(outputs)

        return BigQueryTable(
            self.table_name,
            self.dataset_name,
            self.project_id,
            self.include_dataset,
            self.destroy_protection,
        )
