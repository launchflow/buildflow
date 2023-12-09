import dataclasses
from typing import List

import pulumi
import pulumi_gcp

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import BigQueryDatasetName, GCPProjectID
from buildflow.io.primitive import GCPPrimtive


@dataclasses.dataclass
class BigQueryDataset(GCPPrimtive):
    project_id: GCPProjectID
    dataset_name: BigQueryDatasetName

    def primitive_id(self):
        return f"{self.project_id}.{self.dataset_name}"

    def pulumi_resources(
        self, credentials: GCPCredentials, opts: pulumi.ResourceOptions
    ) -> List[pulumi.Resource]:
        return [
            pulumi_gcp.bigquery.Dataset(
                f"buildflow-{self.dataset_name}",
                project=self.project_id,
                dataset_id=self.dataset_name,
                opts=opts,
            )
        ]

    def cloud_console_url(self) -> str:
        return f"https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1s{self.project_id}!2s{self.dataset_name}&project={self.project_id}"  # noqa
