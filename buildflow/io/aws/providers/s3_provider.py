from typing import Optional, Type

import pulumi
import pulumi_aws

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.types.aws_types import AWSRegion, S3BucketName
from buildflow.core.types.shared_types import FilePath
from buildflow.io.aws.providers.pulumi_providers import aws_provider
from buildflow.io.aws.providers.utils import arn_to_cloud_console_url
from buildflow.io.aws.strategies.s3_strategies import S3BucketSink
from buildflow.io.provider import PulumiProvider, SinkProvider
from buildflow.io.strategies.sink import SinkStrategy
from buildflow.types.portable import FileFormat


class _S3BucketResource(pulumi.ComponentResource):
    def __init__(
        self,
        bucket_name: S3BucketName,
        aws_region: Optional[AWSRegion],
        force_destroy: bool,
        # pulumi_resource options (buildflow internal concept)
        type_: Optional[Type],
        credentials: AWSCredentials,
        opts: pulumi.ResourceOptions,
    ):
        super().__init__(
            "buildflow:aws:s3:Bucket",
            f"buildflow-{bucket_name}",
            None,
            opts,
        )

        outputs = {}

        provider = aws_provider(bucket_name, aws_account_id=None, aws_region=aws_region)
        self.bucket_resource = pulumi_aws.s3.BucketV2(
            opts=pulumi.ResourceOptions(parent=self, provider=provider),
            resource_name=bucket_name,
            bucket=bucket_name,
            force_destroy=force_destroy,
        )
        outputs["aws.s3.bucket"] = self.bucket_resource.id
        outputs["buildflow.cloud_console.url"] = pulumi.Output.all(
            self.bucket_resource.arn
        ).apply(arn_to_cloud_console_url)

        self.register_outputs(outputs)


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
        opts: pulumi.ResourceOptions,
    ) -> _S3BucketResource:
        return _S3BucketResource(
            bucket_name=self.bucket_name,
            aws_region=self.aws_region,
            force_destroy=self.force_destroy,
            # pulumi_resource options (buildflow internal concept)
            type_=type_,
            credentials=credentials,
            opts=opts,
        )
