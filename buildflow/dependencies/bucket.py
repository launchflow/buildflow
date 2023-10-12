from typing import Optional, Union

from buildflow.dependencies.base import Scope, dependency
from buildflow.dependencies.flow_dependencies import FlowCredentials
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.gcp.storage import GCSBucket
from buildflow.io.utils.clients.aws_clients import AWSClients
from buildflow.io.utils.clients.gcp_clients import GCPClients


def BucketDependencyBuilder(
    bucket_primitive: Union[GCSBucket, S3Bucket],
    gcp_quota_project_id: Optional[str] = None,
    aws_region: Optional[str] = None,
):
    PrimDep = bucket_primitive.dependency()

    @dependency(scope=Scope.NO_SCOPE)
    class BucketDependency:
        def __init__(self, bucket: PrimDep, flow_credentials: FlowCredentials) -> None:
            if isinstance(bucket, GCSBucket):
                clients = GCPClients(
                    credentials=flow_credentials.gcp_credentials,
                    quota_project_id=gcp_quota_project_id,
                )
                storage_client = clients.get_storage_client(gcp_quota_project_id)
                self.bucket = storage_client.bucket(bucket.bucket_name)

            elif isinstance(bucket, S3Bucket):
                clients = AWSClients(flow_credentials.aws_credentials, aws_region)
                resource = clients.s3_resource()
                self.bucket = resource.Bucket(bucket.bucket_name)
            else:
                raise ValueError(f"Unknown bucket type: {type(bucket)}")

    return BucketDependency
