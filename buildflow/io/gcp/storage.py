import dataclasses
from typing import List, Optional

import pulumi
import pulumi_gcp

from buildflow.config.cloud_provider_config import GCPOptions
from buildflow.core import utils
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.types.gcp_types import GCPProjectID, GCPRegion, GCSBucketName
from buildflow.core.types.portable_types import BucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.io.gcp.strategies.storage_strategies import GCSBucketSink
from buildflow.io.primitive import GCPPrimtive
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.types.portable import FileFormat

_DEFAULT_BUCKET_LOCATION = "US"


@dataclasses.dataclass
class GCSBucket(GCPPrimtive):
    project_id: GCPProjectID
    bucket_name: GCSBucketName

    # args if you are writing to the bucket as a sink
    file_path: Optional[FilePath] = None
    file_format: Optional[FileFormat] = None

    # pulumi optional args
    # If true destroy will delete the bucket and all contents. If false
    # destroy will fail if the bucket contains data.
    force_destroy: bool = dataclasses.field(default=False, init=False)
    bucket_region: GCPRegion = dataclasses.field(
        default=_DEFAULT_BUCKET_LOCATION, init=False
    )

    @property
    def bucket_url(self):
        return f"gcs://{self.bucket_name}"

    @classmethod
    def from_gcp_options(
        cls,
        gcp_options: GCPOptions,
        *,
        bucket_name: Optional[BucketName] = None,
        file_path: Optional[FilePath] = None,
        file_format: Optional[FileFormat] = None,
    ) -> "GCSBucket":
        project_id = gcp_options.default_project_id
        if project_id is None:
            raise ValueError(
                "No Project ID was provided in the GCP options. Please provide one in "
                "the .buildflow config."
            )
        region = gcp_options.default_region
        if region is None:
            raise ValueError(
                "No Region was provided in the GCP options. Please provide one in "
                "the .buildflow config."
            )
        project_hash = utils.stable_hash(project_id)
        if bucket_name is None:
            bucket_name = f"buildflow_{project_hash[:8]}"
        bucket = cls(
            project_id=project_id,
            bucket_name=bucket_name,
            file_path=file_path,
            file_format=file_format,
        ).options(bucket_region=region)
        bucket.enable_managed()
        return cls(
            project_id=project_id,
            bucket_name=bucket_name,
            file_path=file_path,
            file_format=file_format,
        ).options(bucket_region=region)

    def options(
        self,
        *,
        force_destroy: bool = False,
        bucket_region: GCPRegion = _DEFAULT_BUCKET_LOCATION,
    ) -> "GCSBucket":
        self.force_destroy = force_destroy
        self.bucket_region = bucket_region
        return self

    def sink(self, credentials: GCPCredentials) -> SinkStrategy:
        return GCSBucketSink(
            credentials=credentials,
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            file_path=self.file_path,
            file_format=self.file_format,
        )

    def primitive_id(self):
        return self.bucket_url

    def pulumi_resources(
        self, credentials: GCPCredentials, opts: pulumi.ResourceOptions
    ) -> List[pulumi.Resource]:
        opts = pulumi.ResourceOptions.merge(
            opts,
            pulumi.ResourceOptions(custom_timeouts=pulumi.CustomTimeouts(create="3m")),
        )
        return [
            pulumi_gcp.storage.Bucket(
                resource_name=self.bucket_name,
                name=self.bucket_name,
                location=self.bucket_region,
                project=self.project_id,
                force_destroy=self.force_destroy,
                opts=opts,
            )
        ]

    def cloud_console_url(self) -> str:
        return f"https://console.cloud.google.com/storage/browser/{self.bucket_name}?project={self.project_id}"
