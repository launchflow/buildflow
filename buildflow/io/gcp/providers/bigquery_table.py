from dataclasses import is_dataclass
from typing import Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.types.gcp_types import BigQueryTableID, BigQueryTableName
from buildflow.io.gcp.bigquery_dataset import BigQueryDataset
from buildflow.io.gcp.strategies.bigquery_strategies import StreamingBigQueryTableSink
from buildflow.io.provider import PulumiProvider, SinkProvider
from buildflow.io.utils.schemas import bigquery_schemas


class _BigQueryTablePulumiResource(pulumi.ComponentResource):
    def __init__(
        self,
        dataset: BigQueryDataset,
        table_name: BigQueryTableName,
        destroy_protection: bool,
        schema: Optional[str],
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:gcp:bigquery:Table",
            f"buildflow-{dataset.project_id}-{dataset.dataset_name}-{table_name}",
            None,
            opts,
        )

        outputs = {}
        self.table_resource = pulumi_gcp.bigquery.Table(
            f"buildflow-{table_name}",
            project=dataset.project_id,
            dataset_id=dataset.dataset_name,
            table_id=table_name,
            schema=schema,
            deletion_protection=destroy_protection,
            opts=pulumi.ResourceOptions(parent=self),
        )
        outputs["gcp.bigquery.table_id"] = self.table_resource.id
        outputs[
            "buildflow.cloud_console.url"
        ] = f"https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1s{dataset.project_id}!2s{dataset.dataset_name}!3s{table_name}"

        self.register_outputs(outputs)


class BigQueryTableProvider(SinkProvider, PulumiProvider):
    def __init__(
        self,
        *,
        dataset: BigQueryDataset,
        table_name: BigQueryTableName,
        # sink-only options
        batch_size: str,
        # pulumi-only options
        destroy_protection: bool,
    ):
        self.dataset = dataset
        self.table_name = table_name
        # sink-only options
        self.batch_size = batch_size
        # pulumi-only options
        self.destroy_protection = destroy_protection

    @property
    def table_id(self) -> BigQueryTableID:
        return (
            f"{self.dataset.project_id}.{self.dataset.dataset_name}.{self.table_name}"
        )

    def sink(self, credentials: GCPCredentials):
        return StreamingBigQueryTableSink(
            dataset=self.dataset,
            credentials=credentials,
            table_name=self.table_name,
            batch_size=self.batch_size,
        )

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        # TODO: Maybe throw an error if schema is None
        schema = None
        if hasattr(type_, "__args__"):
            # Using a composite type hint like List or Optional
            type_ = type_.__args__[0]
        if type_ and is_dataclass(type_):
            schema = bigquery_schemas.dataclass_to_json_bq_schema(type_)

        return _BigQueryTablePulumiResource(
            self.dataset,
            self.table_name,
            self.destroy_protection,
            schema,
            type_,
            credentials,
            opts,
        )
