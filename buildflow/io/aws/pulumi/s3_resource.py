from typing import Optional

import pulumi
import pulumi_aws

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.types.aws_types import AWSRegion, S3BucketName
from buildflow.io.aws.pulumi.providers import aws_provider
from buildflow.io.aws.pulumi.utils import arn_to_cloud_console_url


class S3BucketResource(pulumi.ComponentResource):
    def __init__(
        self,
        bucket_name: S3BucketName,
        aws_region: Optional[AWSRegion],
        force_destroy: bool,
        # pulumi_resource options (buildflow internal concept)
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
