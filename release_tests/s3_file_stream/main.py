import os

import buildflow
from buildflow.core.io.aws.s3_file_change_stream import S3FileChangeStream
from buildflow.core.io.aws.s3 import S3Bucket
from buildflow.core.types.aws_types import S3ChangeStreamEventType
from buildflow.core.types.portable_types import (
    FileChangeEvent,
    PortableFileChangeEventType,
)


app = buildflow.Flow(flow_options=buildflow.FlowOptions(require_confirmation=False))


bucket_name = os.getenv("BUCKET_NAME", "my_bucket_name")

source = S3FileChangeStream(
    s3_bucket=S3Bucket(bucket_name=bucket_name, aws_region="us-east-1").options(
        managed=True,
        force_destroy=True,
    ),
    event_types=[
        S3ChangeStreamEventType.OBJECT_CREATED_ALL,
        S3ChangeStreamEventType.OBJECT_REMOVED_ALL,
    ],
)


@app.pipeline(source=source, num_cpus=0.5)
class InputPipeline:
    def process(self, payload: FileChangeEvent):
        if payload.portable_event_type == PortableFileChangeEventType.CREATED:
            print("File created: ", payload.file_path)
        elif payload.portable_event_type == PortableFileChangeEventType.DELETED:
            print("File deleted: ", payload.file_path)
        else:
            # This happens for the initial test event sent by AWS
            print("UNKNOWN EVENT TYPE: ", payload.metadata)
