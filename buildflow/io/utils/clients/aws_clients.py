import logging
from typing import Optional

import boto3
import botocore.exceptions
from botocore import UNSIGNED
from botocore.client import Config

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
        self.use_anonymous_creds = False
        sts_client = self._get_boto_client("sts")
        try:
            # Verify credentials of caller.
            sts_client.get_caller_identity()
        except botocore.exceptions.NoCredentialsError:
            logging.warning(
                "no credentials in environment found, using anonymous credentials"
            )
            self.use_anonymous_creds = True

    def _get_boto_client(self, service_name: str):
        if self.use_anonymous_creds:
            return boto3.client(
                service_name=service_name,
                region_name=self.region,
                config=Config(signature_version=UNSIGNED),
            )
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

    def s3_resource(self):
        if self.use_anonymous_creds:
            return boto3.resource(
                service_name="s3",
                region_name=self.region,
                config=Config(signature_version=UNSIGNED),
            )

        if self.creds.session_token:
            return boto3.resource(
                service_name="s3",
                region_name=self.region,
                aws_session_token=self.creds.session_token,
            )
        return boto3.resource(
            service_name="s3",
            region_name=self.region,
            aws_access_key_id=self.creds.access_key_id,
            aws_secret_access_key=self.creds.secret_access_key,
        )
