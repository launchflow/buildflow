import os
import unittest

import gcsfs
import pytest

from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.options.credentials_options import CredentialsOptions
from buildflow.io.gcp.strategies.storage_strategies import GCSBucketSink
from buildflow.types.portable import FileFormat


@pytest.mark.usefixtures("event_loop_instance")
class StorageStrategiesTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    def setUp(self) -> None:
        os.environ["AWS_ACCESS_KEY_ID"] = "dummy"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "dummy"

        self.bucket_name = "test-bucket"
        self.creds = GCPCredentials(CredentialsOptions.default())
        self.file_path = "test.parquet"
        self.sink = GCSBucketSink(
            project_id="test-project",
            credentials=self.creds,
            bucket_name=self.bucket_name,
            file_path=self.file_path,
            file_format=FileFormat.PARQUET,
        )

    def test_gcs_file_system(self):
        # There's not any good gcs mock library, so we just check that the
        # file_system is a GCSFileSystem.
        self.assertIsInstance(self.sink.file_system, gcsfs.GCSFileSystem)


if __name__ == "__main__":
    unittest.main()
