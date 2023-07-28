from typing import List, Optional, Type

import pulumi
import pulumi_aws

from buildflow.core.providers.provider import (
    PulumiProvider,
)
from buildflow.core.types.aws_types import S3BucketName, AWSRegion
from buildflow.core.resources.pulumi import PulumiResource
from buildflow.core.io.aws.providers.pulumi_providers import aws_provider


class S3BucketProvider(PulumiProvider):
    def __init__(
        self,
        *,
        bucket_name: S3BucketName,
        aws_region: Optional[AWSRegion],
        # sink-only options
        # pulumi-only options
        force_destroy: bool = False,
    ):
        self.bucket_name = bucket_name
        self.aws_region = aws_region
        # sink-only options
        # pulumi-only options
        self.force_destroy = force_destroy

    def pulumi_resources(
        self, type_: Optional[Type], depends_on: List[PulumiResource] = []
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
        pulumi.export("aws.s3.bucket_id", self.bucket_name)
        return [
            PulumiResource(
                resource_id=self.bucket_name,
                resource=bucket_resource,
                exports={
                    "aws.s3.bucket_id": self.bucket_name,
                },
            )
        ]
