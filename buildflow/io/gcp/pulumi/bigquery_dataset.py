import pulumi
import pulumi_gcp

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import BigQueryDatasetName, GCPProjectID


class BigqueryDatasetPulumiResource(pulumi.ComponentResource):
    def __init__(
        self,
        dataset_name: BigQueryDatasetName,
        project_id: GCPProjectID,
        # pulumi_resource options (buildflow internal concept)
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:gcp:bigquery:Dataset",
            f"buildflow-{project_id}-{dataset_name}",
            None,
            opts,
        )
        outputs = {}
        self.dataset_resource = pulumi_gcp.bigquery.Dataset(
            f"buildflow-{dataset_name}",
            project=project_id,
            dataset_id=dataset_name,
            opts=pulumi.ResourceOptions(parent=self),
        )
        outputs["gcp.bigquery.dataset_id"] = self.dataset_resource.id
        outputs[
            "buildflow.cloud_console.url"
        ] = f"https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1s{project_id}!2s{dataset_name}&project={project_id}"
        self.register_outputs(outputs)
