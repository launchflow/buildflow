from typing import List, Optional, Type

import pulumi
import pulumi_gcp

from buildflow.core.credentials import GCPCredentials
from buildflow.core.io.gcp.strategies.storage_strategies import GCSBucketSink
from buildflow.core.providers.provider import PulumiProvider, SinkProvider
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.types.gcp_types import GCPProjectID, GCPRegion, GCSBucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.types.portable import FileFormat


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

    def pulumi_resources(
        self,
        type_: Optional[Type],
        credentials: GCPCredentials,
        depends_on: List[PulumiResource] = [],
    ):
        del type_
        bucket_resource = pulumi_gcp.storage.Bucket(
            resource_name=self.bucket_name,
            name=self.bucket_name,
            location=self.bucket_region,
            project=self.project_id,
            force_destroy=self.force_destroy,
        )
        pulumi.export("gcp.storage.bucket_id", self.bucket_name)
        return [
            PulumiResource(
                resource_id=self.bucket_name,
                resource=bucket_resource,
                exports={
                    "gcp.storage.bucket_id": self.bucket_name,
                },
            )
        ]
