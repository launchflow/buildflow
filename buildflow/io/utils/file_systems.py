import json
from typing import Union

import fsspec
import gcsfs
import s3fs
from fsspec.implementations.local import LocalFileSystem

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials


def get_file_system(
    credentials: Union[AWSCredentials, GCPCredentials]
) -> fsspec.AbstractFileSystem:
    if isinstance(credentials, AWSCredentials):
        return s3fs.S3FileSystem(
            key=credentials.access_key_id,
            secret=credentials.secret_access_key,
        )
    elif isinstance(credentials, GCPCredentials):
        if credentials.service_account_info is not None:
            token = json.dumps(credentials.service_account_info)
        else:
            token = None
        return gcsfs.GCSFileSystem(token=token)
    elif isinstance(credentials, EmptyCredentials):
        return LocalFileSystem()
