import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.types.gcp_types import BigQueryTableName
from buildflow.io.gcp.bigquery_dataset import BigQueryDataset


class BigQueryTablePulumiResource(pulumi.ComponentResource):
    def __init__(
        self,
        dataset: BigQueryDataset,
        table_name: BigQueryTableName,
        destroy_protection: bool,
        schema: str,
        # pulumi_resource options (buildflow internal concept)
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
