import dataclasses
from typing import Optional

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core import utils
from buildflow.core.types.gcp_types import BigQueryTableID, BigQueryTableName
from buildflow.core.types.portable_types import TableName
from buildflow.io.gcp.bigquery_dataset import BigQueryDataset
from buildflow.io.gcp.providers.bigquery_table import BigQueryTableProvider
from buildflow.io.primitive import GCPPrimtive, Primitive

_DEFAULT_DESTROY_PROTECTION = False
_DEFAULT_BATCH_SIZE = 10_000


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
    dataset: BigQueryDataset
    table_name: BigQueryTableName
    batch_size: int = dataclasses.field(default=_DEFAULT_BATCH_SIZE, init=False)
    destroy_protection: bool = dataclasses.field(
        default=_DEFAULT_DESTROY_PROTECTION, init=False
    )

    @property
    def table_id(self) -> BigQueryTableID:
        return (
            f"{self.dataset.project_id}.{self.dataset.dataset_name}.{self.table_name}"
        )

    def options(
        self,
        # Pulumi management options
        managed: bool = False,
        destroy_protection: bool = _DEFAULT_DESTROY_PROTECTION,
        # Sink options
        batch_size: int = _DEFAULT_BATCH_SIZE,
    ) -> Primitive:
        to_ret = super().options(managed)
        to_ret.destroy_protection = destroy_protection
        to_ret.batch_size = batch_size
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
            dataset=BigQueryDataset(
                project_id=project_id, dataset_name="buildflow_managed"
            ),
            table_name=table_name,
        )

    def sink_provider(self) -> BigQueryTableProvider:
        # TODO: Add support to supply the sink-only options
        return BigQueryTableProvider(
            dataset=self.dataset,
            table_name=self.table_name,
            batch_size=self.batch_size,
            destroy_protection=self.destroy_protection,
        )

    def _pulumi_provider(self) -> BigQueryTableProvider:
        return BigQueryTableProvider(
            dataset=self.dataset,
            table_name=self.table_name,
            batch_size=self.batch_size,
            destroy_protection=self.destroy_protection,
        )
