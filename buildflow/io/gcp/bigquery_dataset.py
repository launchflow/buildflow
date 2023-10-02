import dataclasses

import pulumi

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import BigQueryDatasetName, GCPProjectID
from buildflow.io.gcp.pulumi.bigquery_dataset import BigqueryDatasetPulumiResource
from buildflow.io.primitive import GCPPrimtive


@dataclasses.dataclass
class BigQueryDataset(GCPPrimtive):
    project_id: GCPProjectID
    dataset_name: BigQueryDatasetName

    def pulumi_resource(
        self, credentials: GCPCredentials, opts: pulumi.ResourceOptions
    ) -> BigqueryDatasetPulumiResource:
        return BigqueryDatasetPulumiResource(
            project_id=self.project_id,
            dataset_name=self.dataset_name,
            credentials=credentials,
            opts=opts,
        )

    def cloud_console_url(self) -> str:
        return f"https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1s{self.project_id}!2s{self.dataset_name}&project={self.project_id}"  # noqa
