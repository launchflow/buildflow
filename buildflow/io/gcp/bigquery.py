import dataclasses
from typing import Optional

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core import utils
from buildflow.core.types.gcp_types import (
    BigQueryDatasetName,
    BigQueryTableID,
    BigQueryTableName,
    GCPProjectID,
)
from buildflow.core.types.portable_types import TableName
from buildflow.io.gcp.providers.bigquery_table import BigQueryTableProvider
from buildflow.io.primitive import GCPPrimtive, Primitive

_DEFAULT_INCLUDE_DATASET = True
_DEFAULT_DESTROY_PROTECTION = False


@dataclasses.dataclass
class BigQueryTable(
    GCPPrimtive[
        # Pulumi provider type
        BigQueryTableProvider,
        # Source provider type
        None,
        # Sink provider type
        BigQueryTableProvider,
        # Background task provider type
        None,
    ]
):
    project_id: GCPProjectID
    dataset_name: BigQueryDatasetName
    table_name: BigQueryTableName
    batch_size: int = 10_000
    include_dataset: bool = dataclasses.field(
        default=_DEFAULT_INCLUDE_DATASET, init=False
    )
    destroy_protection: bool = dataclasses.field(
        default=_DEFAULT_DESTROY_PROTECTION, init=False
    )

    @property
    def table_id(self) -> BigQueryTableID:
        return f"{self.project_id}.{self.dataset_name}.{self.table_name}"

    def options(
        self,
        managed: bool = False,
        include_dataset: bool = _DEFAULT_INCLUDE_DATASET,
        destroy_protection: bool = False,
    ) -> Primitive:
        to_ret = super().options(managed)
        to_ret.include_dataset = include_dataset
        to_ret.destroy_protection = destroy_protection
        return to_ret

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
            project_id=project_id,
            dataset_name="buildflow_managed",
            table_name=table_name,
        )

    def sink_provider(self) -> BigQueryTableProvider:
        # TODO: Add support to supply the sink-only options
        return BigQueryTableProvider(
            project_id=self.project_id,
            dataset_name=self.dataset_name,
            table_name=self.table_name,
            batch_size=self.batch_size,
            include_dataset=self.include_dataset,
            destroy_protection=self.destroy_protection,
        )

    def _pulumi_provider(self) -> BigQueryTableProvider:
        return BigQueryTableProvider(
            project_id=self.project_id,
            dataset_name=self.dataset_name,
            table_name=self.table_name,
            batch_size=self.batch_size,
            include_dataset=self.include_dataset,
            destroy_protection=self.destroy_protection,
        )
