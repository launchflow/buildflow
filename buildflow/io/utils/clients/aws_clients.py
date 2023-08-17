from typing import Optional

import boto3

from buildflow.core.credentials import AWSCredentials
from buildflow.core.types.aws_types import AWSRegion


class AWSClients:
    def __init__(
        self,
        credentials: AWSCredentials,
        region: Optional[AWSRegion],
    ) -> None:
        # TODO: add some caching for clients
        self.creds = credentials
        self.region = region

    def _get_boto_client(self, service_name: str):
        if self.creds.session_token:
            return boto3.client(
                service_name=service_name,
                region_name=self.region,
                aws_session_token=self.creds.session_token,
            )
        return boto3.client(
            service_name=service_name,
            region_name=self.region,
            aws_access_key_id=self.creds.access_key_id,
            aws_secret_access_key=self.creds.secret_access_key,
        )

    def sqs_client(self):
        return self._get_boto_client("sqs")

    def s3_client(self):
        return self._get_boto_client("s3")
