import dataclasses
from typing import Optional

import pulumi

from buildflow.config.cloud_provider_config import AWSOptions
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.types.aws_types import AWSRegion, S3BucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.io.aws.pulumi.s3_resource import S3BucketResource
from buildflow.io.aws.strategies.s3_strategies import S3BucketSink
from buildflow.io.primitive import AWSPrimtive
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.types.portable import FileFormat


@dataclasses.dataclass
class S3Bucket(AWSPrimtive):
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

    def options(self, *, force_destroy: bool = False) -> "S3Bucket":
        self.force_destroy = force_destroy
        return self

    def sink(self, credentials: AWSCredentials) -> SinkStrategy:
        return S3BucketSink(
            credentials, self.bucket_name, self.file_path, self.file_format
        )

    def pulumi_resource(
        self, credentials: AWSCredentials, opts: pulumi.ResourceOptions
    ) -> S3BucketResource:
        return S3BucketResource(
            self.bucket_name, self.aws_region, self.force_destroy, credentials, opts
        )
