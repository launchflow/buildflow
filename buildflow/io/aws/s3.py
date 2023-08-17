import dataclasses
from typing import Optional

from buildflow.config.cloud_provider_config import AWSOptions
from buildflow.core.types.aws_types import AWSRegion, S3BucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.io.aws.providers.s3_provider import S3BucketProvider
from buildflow.io.primitive import AWSPrimtive
from buildflow.io.provider import PulumiProvider, SinkProvider
from buildflow.types.portable import FileFormat


@dataclasses.dataclass
class S3Bucket(
    AWSPrimtive[
        # Pulumi provider type
        S3BucketProvider,
        # Source provider type
        None,
        # Sink provider type
        S3BucketProvider,
        # Background task provider type
        None,
    ]
):
    bucket_name: S3BucketName

    # args if you are writing to the bucket as a sink
    file_path: Optional[FilePath] = None
    file_format: Optional[FileFormat] = None

    # optional args
    aws_region: Optional[AWSRegion] = None

    # pulumi args
    # If true destroy will delete the bucket and all contents. If false
    # destroy will fail if the bucket contains data.
    force_destroy: bool = dataclasses.field(default=False, init=False)

    @property
    def bucket_url(self):
        return f"s3://{self.bucket_name}"

    @classmethod
    def from_aws_options(
        cls, aws_options: AWSOptions, *, bucket_name: S3BucketName
    ) -> "S3Bucket":
        region = aws_options.default_region
        return cls(bucket_name=bucket_name, aws_region=region)

    def options(self, *, managed: bool = False, force_destroy: bool = False):
        to_ret = super().options(managed)
        to_ret.force_destroy = force_destroy
        return to_ret

    def sink_provider(self) -> SinkProvider:
        return S3BucketProvider(
            bucket_name=self.bucket_name,
            aws_region=self.aws_region,
            force_destroy=self.force_destroy,
            file_path=self.file_path,
            file_format=self.file_format,
        )

    def _pulumi_provider(self) -> PulumiProvider:
        return S3BucketProvider(
            bucket_name=self.bucket_name,
            aws_region=self.aws_region,
            force_destroy=self.force_destroy,
            file_path=self.file_path,
            file_format=self.file_format,
        )
