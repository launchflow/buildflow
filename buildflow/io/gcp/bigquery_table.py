import dataclasses
from typing import List, Optional, Type

import pulumi
import pulumi_gcp

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core import utils
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import BigQueryTableID, BigQueryTableName
from buildflow.core.types.portable_types import TableName
from buildflow.io.gcp.bigquery_dataset import BigQueryDataset
from buildflow.io.gcp.strategies.bigquery_strategies import StreamingBigQueryTableSink
from buildflow.io.primitive import GCPPrimtive, Primitive
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.io.utils.schemas import bigquery_schemas

_DEFAULT_DESTROY_PROTECTION = False
_DEFAULT_BATCH_SIZE = 10_000


@dataclasses.dataclass
class BigQueryTable(GCPPrimtive):
    dataset: BigQueryDataset
    table_name: BigQueryTableName
    batch_size: int = dataclasses.field(default=_DEFAULT_BATCH_SIZE, init=False)
    destroy_protection: bool = dataclasses.field(
        default=_DEFAULT_DESTROY_PROTECTION, init=False
    )
    schema: Optional[Type] = dataclasses.field(default=None, init=False)

    @property
    def table_id(self) -> BigQueryTableID:
        return (
            f"{self.dataset.project_id}.{self.dataset.dataset_name}.{self.table_name}"
        )

    def primitive_id(self):
        return self.table_id

    def options(
        self,
        # Pulumi management options
        destroy_protection: bool = _DEFAULT_DESTROY_PROTECTION,
        schema: Optional[Type] = None,
        # Sink options
        batch_size: int = _DEFAULT_BATCH_SIZE,
    ) -> Primitive:
        self.schema = schema
        self.batch_size = batch_size
        self.destroy_protection = destroy_protection
        return self

    @classmethod
    def from_gcp_options(
        cls,
        gcp_options: GCPOptions,
        *,
        table_name: Optional[TableName] = None,
    ) -> "BigQueryTable":
        project_id = gcp_options.default_project_id
        if project_id is None:
            raise ValueError(
                "No Project ID was provided in the GCP options. Please provide one in "
                "the .buildflow config."
            )
        project_hash = utils.stable_hash(project_id)
        if table_name is None:
            table_name = f"table_{project_hash[:8]}"
        return cls(
            dataset=BigQueryDataset(
                project_id=project_id, dataset_name="buildflow_managed"
            ),
            table_name=table_name,
        )

    def sink(self, credentials: GCPCredentials) -> SinkStrategy:
        return StreamingBigQueryTableSink(
            credentials=credentials,
            dataset=self.dataset,
            table_name=self.table_name,
            batch_size=self.batch_size,
        )

    def pulumi_resources(
        self, credentials: GCPCredentials, opts: pulumi.ResourceOptions
    ) -> List[pulumi.Resource]:
        schema = None
        if self.schema is None:
            raise ValueError(
                "Schema is required to create a bigquery table. "
                "Pass one in with: `BigQueryTable(...).options(schema=MyDataClass)`"
            )
        type_ = self.schema
        if hasattr(type_, "__args__"):
            # Using a composite type hint like List or Optional
            type_ = type_.__args__[0]
        if type_ and dataclasses.is_dataclass(type_):
            schema = bigquery_schemas.dataclass_to_json_bq_schema(type_)
        else:
            raise ValueError(
                "Could not determine schema for BigQuery table. "
                "Was the schema you passed in a dataclass?"
            )
        return [
            pulumi_gcp.bigquery.Table(
                f"buildflow-{self.table_name}",
                project=self.dataset.project_id,
                dataset_id=self.dataset.dataset_name,
                table_id=self.table_name,
                schema=schema,
                deletion_protection=self.destroy_protection,
                opts=opts,
            )
        ]

    def cloud_console_url(self) -> str:
        return f"https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1s{self.dataset.project_id}!2s{self.dataset.dataset_name}!3s{self.table_name}"
