import dataclasses

from buildflow.core.types.gcp_types import BigQueryDatasetName, GCPProjectID
from buildflow.io.gcp.providers.bigquery_dataset import BigQueryDatasetProvider
from buildflow.io.primitive import GCPPrimtive


@dataclasses.dataclass
class BigQueryDataset(GCPPrimtive):
    project_id: GCPProjectID
    dataset_name: BigQueryDatasetName

    def _pulumi_provider(self) -> BigQueryDatasetProvider:
        return BigQueryDatasetProvider(
            project_id=self.project_id, dataset_name=self.dataset_name
        )
