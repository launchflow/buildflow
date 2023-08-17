from typing import Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.types.gcp_types import GCPProjectID, GCPRegion, GCSBucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.io.gcp.strategies.storage_strategies import GCSBucketSink
from buildflow.io.provider import PulumiProvider, SinkProvider
from buildflow.types.portable import FileFormat


class _GCPStoragePulumiResource(pulumi.ComponentResource):
    def __init__(
        self,
        bucket_name: GCSBucketName,
        bucket_region: GCPRegion,
        project_id: GCPProjectID,
        force_destroy: bool,
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
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


class GCSBucketProvider(SinkProvider, PulumiProvider):
    def __init__(
        self,
        *,
        project_id: GCPProjectID,
        bucket_name: GCSBucketName,
        bucket_region: GCPRegion,
        # sink-only options
        file_path: Optional[FilePath] = None,
        file_format: Optional[FileFormat] = None,
        # pulumi-only options
        force_destroy: bool = False,
    ):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        # sink-only options
        self.file_path = file_path
        self.file_format = file_format
        # pulumi-only options
        self.force_destroy = force_destroy

    @property
    def bucket_url(self):
        return f"s3://{self.bucket_name}"

    def sink(self, credentials: GCPCredentials):
        return GCSBucketSink(
            credentials=credentials,
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            file_path=self.file_path,
            file_format=self.file_format,
        )

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
        opts: pulumi.ResourceOptions,
    ):
        return _GCPStoragePulumiResource(
            bucket_name=self.bucket_name,
            bucket_region=self.bucket_region,
            project_id=self.project_id,
            force_destroy=self.force_destroy,
            type_=type_,
            credentials=credentials,
            opts=opts,
        )
