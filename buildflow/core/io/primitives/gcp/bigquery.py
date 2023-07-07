import dataclasses

from buildflow.core import utils
from buildflow.core.io.primitives.primitive import Primitive
from buildflow.core.options.primitive_options import GCPOptions
from buildflow.core.types.gcp_types import DatasetName, ProjectID, TableID, TableName
from buildflow.core.io.primitives.gcp.providers.bigquery_providers import (
    BigQueryTableProvider,
)


@dataclasses.dataclass
class BigQueryTable(Primitive):
    project_id: ProjectID
    dataset_name: DatasetName
    table_name: TableName

    @property
    def table_id(self) -> TableID:
        return f"{self.project_id}.{self.dataset_name}.{self.table_name}"

    @classmethod
    def from_options(cls, options: GCPOptions) -> "BigQueryTable":
        project_id = options.default_project_id
        project_hash = utils.stable_hash(project_id)
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
        )

    def pulumi_provider(self) -> BigQueryTableProvider:
        # TODO: Add support to supply the pulumi-only options
        return BigQueryTableProvider(
            project_id=self.project_id,
            dataset_name=self.dataset_name,
            table_name=self.table_name,
        )
