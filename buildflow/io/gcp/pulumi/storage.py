import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.types.gcp_types import GCPProjectID, GCPRegion, GCSBucketName


class GCPStoragePulumiResource(pulumi.ComponentResource):
    def __init__(
        self,
        bucket_name: GCSBucketName,
        bucket_region: GCPRegion,
        project_id: GCPProjectID,
        force_destroy: bool,
        # pulumi_resource options (buildflow internal concept)
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:gcp:storage:Bucket",
            f"buildflow-{bucket_name}",
            None,
            opts,
        )

        outputs = {}

        self.bucket_resource = pulumi_gcp.storage.Bucket(
            resource_name=bucket_name,
            name=bucket_name,
            location=bucket_region,
            project=project_id,
            force_destroy=force_destroy,
            opts=pulumi.ResourceOptions(
                parent=self,
                depends_on=[],
                custom_timeouts=pulumi.CustomTimeouts(create="3m"),
            ),
        )

        outputs["gcp.storage.bucket"] = self.bucket_resource.id
        outputs[
            "buildflow.cloud_console.url"
        ] = f"https://console.cloud.google.com/storage/browser/{bucket_name}?project={project_id}"

        self.register_outputs(outputs)
