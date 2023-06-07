from dataclasses import is_dataclass
from typing import Any, Callable, Dict, Optional, List, Type

import pulumi
import pulumi_gcp

from buildflow.io.providers import PulumiProvider, PushProvider
from buildflow.io.providers.base import PulumiResources
from buildflow.io.providers.gcp.utils import clients as gcp_clients
from buildflow.io.providers.schemas import bigquery_schemas
from buildflow.io.providers.schemas import converters


class StreamingBigQueryProvider(PushProvider, PulumiProvider):
    # TODO: should make this configure able.
    # We should probably also chunk this up based on data size instead
    # of just number of rows. If the data is too big, we will get an error
    # from BigQuery.
    _BATCH_SIZE = 10_000

    def __init__(
        self,
        *,
        project_id: str,
        table_id: str,
        include_dataset: bool = True,
        destroy_protection: bool = True,
    ):
        super().__init__()
        # configuration
        self.billing_project = project_id
        self.table_id = table_id
        self.include_dataset = include_dataset
        self.destroy_protection = destroy_protection
        # setup
        self.bq_client = gcp_clients.get_bigquery_client(self.billing_project)

    async def push(self, batch: List[dict]):
        for i in range(0, len(batch), self._BATCH_SIZE):
            rows = batch[i : i + self._BATCH_SIZE]
            errors = self.bq_client.insert_rows_json(self.table_id, rows)
            if errors:
                raise RuntimeError(f"BigQuery streaming insert failed: {errors}")

    def push_converter(
        self, user_defined_type: Optional[Type]
    ) -> Callable[[Any], Dict[str, Any]]:
        return converters.json_push_converter(user_defined_type)

    def pulumi(
        self,
        type_: Optional[Type],
    ) -> PulumiResources:
        # TODO: add additional options for dataset and table creation.
        project, dataset, table = self.table_id.split(".")
        resources = []
        exports = {}
        if self.include_dataset:
            dataset_resource = pulumi_gcp.bigquery.Dataset(
                resource_name=f"{project}.{dataset}",
                project=project,
                dataset_id=dataset,
            )
            resources.append(dataset_resource)
            exports["gcp.bigquery.dataset_id"] = f"{project}.{dataset}"

        schema = None
        if hasattr(type_, "__args__"):
            # Using a composite type hint like List or Optional
            type_ = type_.__args__[0]
        if type_ and is_dataclass(type_):
            schema = bigquery_schemas.dataclass_to_json_bq_schema(type_)

        parent = dataset_resource if self.include_dataset else None
        table_resource = pulumi_gcp.bigquery.Table(
            opts=pulumi.ResourceOptions(parent=parent),
            resource_name=self.table_id,
            project=project,
            dataset_id=dataset,
            table_id=table,
            schema=schema,
            deletion_protection=(
                self.destroy_protection if not self.destroy_protection else None
            ),
        )

        resources.append(table_resource)
        exports["gcp.biquery.table_id"] = self.table_id

        return PulumiResources(resources=resources, exports=exports)
