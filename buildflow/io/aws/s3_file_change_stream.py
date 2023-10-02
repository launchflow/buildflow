import dataclasses
from typing import Iterable

import pulumi

from buildflow.config.cloud_provider_config import AWSOptions
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.types.aws_types import S3BucketName
from buildflow.io.aws.pulumi.s3_file_change_stream_resource import (
    S3FileChangeStreamResource,
)
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.aws.sqs import SQSQueue
from buildflow.io.aws.strategies.s3_file_change_stream_strategies import (
    S3FileChangeStreamSource,
)
from buildflow.io.primitive import AWSPrimtive
from buildflow.io.strategies.source import SourceStrategy
from buildflow.types.aws import S3ChangeStreamEventType


@dataclasses.dataclass
class S3FileChangeStream(AWSPrimtive):
    s3_bucket: S3Bucket
    event_types: Iterable[S3ChangeStreamEventType] = (
        S3ChangeStreamEventType.OBJECT_CREATED_ALL,
    )

    # The sqs queue is always managed by the S3FileChangeStream and
    # is setup in __post_init__ based on the bucket configuration.
    sqs_queue: SQSQueue = dataclasses.field(init=False)

    def __post_init__(self):
        self.sqs_queue = SQSQueue(
            queue_name=f"{self.s3_bucket.bucket_name}_queue",
            aws_region=self.s3_bucket.aws_region,
        )
        self.sqs_queue.enable_managed()

    @classmethod
    def from_aws_options(
        cls, aws_options: AWSOptions, bucket_name: S3BucketName
    ) -> AWSPrimtive:
        bucket = S3Bucket.from_aws_options(aws_options, bucket_name=bucket_name)
        bucket.enable_managed()
        return cls(bucket)

    def source(self, credentials: AWSCredentials) -> SourceStrategy:
        return S3FileChangeStreamSource(
            credentials=credentials,
            sqs_source=self.sqs_queue.source(credentials),
            aws_region=self.s3_bucket.aws_region,
        )

    def pulumi_resource(
        self, credentials: AWSCredentials, opts: pulumi.ResourceOptions
    ):
        return S3FileChangeStreamResource(
            self.s3_bucket, self.sqs_queue, self.event_types, credentials, opts
        )
