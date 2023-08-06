import dataclasses
import unittest

import pulumi_aws
import pulumi_snowflake

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.io.aws.providers.s3_provider import S3BucketProvider
from buildflow.core.io.snowflake.providers.table_provider import SnowflakeTableProvider
from buildflow.core.options.credentials_options import (
    AWSCredentialsOptions,
    CredentialsOptions,
)


@dataclasses.dataclass
class SnowflakeTableType:
    val: int


class SnowflakeTableProviderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.creds = AWSCredentials(
            CredentialsOptions(
                gcp_credentials_options=None,
                aws_credentials_options=AWSCredentialsOptions(
                    access_key_id="access",
                    secret_access_key="secret",
                    session_token=None,
                ),
            )
        )

    def test_pulumi_resources_all_managed(self):
        provider = SnowflakeTableProvider(
            table="table",
            database="database",
            schema="schema",
            flush_time_secs=10,
            bucket_provider=S3BucketProvider(
                file_format=None,
                file_path=None,
                bucket_name="bucket",
                aws_region=None,
            ),
            snow_pipe="snow_pipe",
            snowflake_stage="snowflake_stage",
            database_managed=True,
            schema_managed=True,
            bucket_managed=True,
            snow_pipe_managed=True,
            stage_managed=True,
            account="account",
            user="user",
            private_key="private_key",
        )

        resources = provider.pulumi_resources(
            type_=SnowflakeTableType, credentials=self.creds, depends_on=[]
        )

        self.assertEqual(len(resources), 6)

        self.assertIsInstance(resources[0].resource, pulumi_aws.s3.BucketV2)
        self.assertIsInstance(resources[1].resource, pulumi_snowflake.Database)
        self.assertIsInstance(resources[2].resource, pulumi_snowflake.Schema)
        self.assertIsInstance(resources[3].resource, pulumi_snowflake.Table)
        self.assertIsInstance(resources[4].resource, pulumi_snowflake.Stage)
        self.assertIsInstance(resources[5].resource, pulumi_snowflake.Pipe)

    def test_pulumi_resources_all_unmanaged(self):
        provider = SnowflakeTableProvider(
            table="table",
            database="database",
            schema="schema",
            flush_time_secs=10,
            bucket_provider=S3BucketProvider(
                file_format=None,
                file_path=None,
                bucket_name="bucket",
                aws_region=None,
            ),
            snow_pipe="snow_pipe",
            snowflake_stage="snowflake_stage",
            database_managed=False,
            schema_managed=False,
            bucket_managed=False,
            snow_pipe_managed=False,
            stage_managed=False,
            account="account",
            user="user",
            private_key="private_key",
        )

        resources = provider.pulumi_resources(
            type_=SnowflakeTableType, credentials=self.creds, depends_on=[]
        )

        self.assertEqual(len(resources), 1)

        self.assertIsInstance(resources[0].resource, pulumi_snowflake.Table)

    def test_pulumu_resources_no_type(self):
        provider = SnowflakeTableProvider(
            table="table",
            database="database",
            schema="schema",
            flush_time_secs=10,
            bucket_provider=S3BucketProvider(
                file_format=None,
                file_path=None,
                bucket_name="bucket",
                aws_region=None,
            ),
            snow_pipe="snow_pipe",
            snowflake_stage="snowflake_stage",
            database_managed=False,
            schema_managed=False,
            bucket_managed=False,
            snow_pipe_managed=False,
            stage_managed=False,
            account="account",
            user="user",
            private_key="private_key",
        )

        with self.assertRaises(ValueError):
            provider.pulumi_resources(type_=None, credentials=self.creds, depends_on=[])

    def test_pulumu_resources_non_dataclass_type(self):
        provider = SnowflakeTableProvider(
            table="table",
            database="database",
            schema="schema",
            flush_time_secs=10,
            bucket_provider=S3BucketProvider(
                file_format=None,
                file_path=None,
                bucket_name="bucket",
                aws_region=None,
            ),
            snow_pipe="snow_pipe",
            snowflake_stage="snowflake_stage",
            database_managed=False,
            schema_managed=False,
            bucket_managed=False,
            snow_pipe_managed=False,
            stage_managed=False,
            account="account",
            user="user",
            private_key="private_key",
        )

        with self.assertRaises(ValueError):
            provider.pulumi_resources(type_=int, credentials=self.creds, depends_on=[])


if __name__ == "__main__":
    unittest.main()
