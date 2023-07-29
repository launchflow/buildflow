import dataclasses
from typing import Optional

from buildflow.config.cloud_provider_config import AWSOptions
from buildflow.core.io.aws.providers.s3_provider import S3BucketProvider
from buildflow.core.io.primitive import AWSPrimtive
from buildflow.core.providers.provider import PulumiProvider, SinkProvider
from buildflow.core.types.aws_types import AWSRegion, S3BucketName


@dataclasses.dataclass
class S3Bucket(AWSPrimtive):
    bucket_name: S3BucketName

    # optional args
    aws_region: Optional[AWSRegion] = None

    # pulumi args
    # If true destroy will delete the bucket and all contents. If false
    # destroy will fail if the bucket contains data.
    force_destroy: bool = dataclasses.field(default=False, init=False)

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
        raise NotImplementedError("Writing to an s3 bucket is currently not supported")

    def pulumi_provider(self) -> PulumiProvider:
        return S3BucketProvider(
            bucket_name=self.bucket_name,
            aws_region=self.aws_region,
            force_destroy=self.force_destroy,
        )
