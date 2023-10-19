import dataclasses
from typing import List, Optional

import pulumi
import pulumi_aws

from buildflow.config.cloud_provider_config import AWSOptions
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.types.aws_types import AWSRegion, S3BucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.io.aws.pulumi.providers import aws_provider
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

    def primitive_id(self):
        return self.bucket_url

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

    def pulumi_resources(
        self, credentials: AWSCredentials, opts: pulumi.ResourceOptions
    ) -> List[pulumi.Resource]:
        provider = aws_provider(
            self.bucket_name, aws_account_id=None, aws_region=self.aws_region
        )
        opts = pulumi.ResourceOptions.merge(
            opts, pulumi.ResourceOptions(provider=provider)
        )
        bucket_resource = pulumi_aws.s3.BucketV2(
            opts=opts,
            resource_name=self.bucket_name,
            bucket=self.bucket_name,
            force_destroy=self.force_destroy,
        )
        return [bucket_resource]
