from typing import List, Optional, Type

import pulumi
import pulumi_aws

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.io.aws.providers.pulumi_providers import aws_provider
from buildflow.core.io.aws.strategies.s3_strategies import S3BucketSink
from buildflow.core.providers.provider import PulumiProvider, SinkProvider
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.strategies.sink import SinkStrategy
from buildflow.core.types.aws_types import AWSRegion, S3BucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.types.portable import FileFormat


class S3BucketProvider(PulumiProvider, SinkProvider):
    def __init__(
        self,
        *,
        file_path: Optional[FilePath],
        file_format: Optional[FileFormat],
        bucket_name: S3BucketName,
        aws_region: Optional[AWSRegion],
        # sink-only options
        # pulumi-only options
        force_destroy: bool = False,
    ):
        self.bucket_name = bucket_name
        self.aws_region = aws_region
        # sink-only options
        self.file_path = file_path
        self.file_format = file_format
        # pulumi-only options
        self.force_destroy = force_destroy

    @property
    def bucket_url(self):
        return f"s3://{self.bucket_name}"

    def sink(self, credentials: AWSCredentials) -> SinkStrategy:
        return S3BucketSink(
            credentials=credentials,
            bucket_name=self.bucket_name,
            file_path=self.file_path,
            file_format=self.file_format,
        )

    def pulumi_resource(
        self,
        type_: Optional[Type],
        credentials: AWSCredentials,
        depends_on: List[PulumiResource] = [],
    ):
        provider = aws_provider(
            self.bucket_name, aws_account_id=None, aws_region=self.aws_region
        )
        depends = [tr.resource for tr in depends_on]
        bucket_resource = pulumi_aws.s3.BucketV2(
            opts=pulumi.ResourceOptions(depends_on=depends, provider=provider),
            resource_name=self.bucket_name,
            bucket=self.bucket_name,
            force_destroy=self.force_destroy,
        )
        pulumi.export(f"aws.s3.bucket_id.{self.bucket_name}", self.bucket_name)
        return [
            PulumiResource(
                resource_id=self.bucket_name,
                resource=bucket_resource,
                exports={
                    f"aws.s3.bucket_id.{self.bucket_name}": self.bucket_name,
                },
            )
        ]
