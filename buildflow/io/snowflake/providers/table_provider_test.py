import dataclasses
import unittest

import pulumi
import pulumi_snowflake

from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.options.credentials_options import (
    AWSCredentialsOptions,
    CredentialsOptions,
)
from buildflow.io.aws.s3 import S3Bucket
from buildflow.io.snowflake.providers.table_provider import SnowflakeTableProvider


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
            bucket=S3Bucket(bucket_name="bucket").options(managed=True),
            snow_pipe="snow_pipe",
            snowflake_stage="snowflake_stage",
            database_managed=True,
            schema_managed=True,
            snow_pipe_managed=True,
            stage_managed=True,
            account="account",
            user="user",
            private_key="private_key",
        )

        resource = provider.pulumi_resource(
            type_=SnowflakeTableType,
            credentials=self.creds,
            opts=pulumi.ResourceOptions(),
        )

        child_resource = resource._childResources
        self.assertEqual(len(child_resource), 5)

        self.assertIsInstance(resource.database_resource, pulumi_snowflake.Database)
        self.assertIsInstance(resource.schema_resource, pulumi_snowflake.Schema)
        self.assertIsInstance(resource.table_resource, pulumi_snowflake.Table)
        self.assertIsInstance(resource.stage_resource, pulumi_snowflake.Stage)
        self.assertIsInstance(resource.snow_pipe_resource, pulumi_snowflake.Pipe)

    def test_pulumi_resources_all_unmanaged(self):
        provider = SnowflakeTableProvider(
            table="table",
            database="database",
            schema="schema",
            flush_time_secs=10,
            bucket=S3Bucket(bucket_name="bucket"),
            snow_pipe="snow_pipe",
            snowflake_stage="snowflake_stage",
            database_managed=False,
            schema_managed=False,
            snow_pipe_managed=False,
            stage_managed=False,
            account="account",
            user="user",
            private_key="private_key",
        )

        resource = provider.pulumi_resource(
            type_=SnowflakeTableType,
            credentials=self.creds,
            opts=pulumi.ResourceOptions(),
        )

        self.assertEqual(len(resource._childResources), 1)
        self.assertIsInstance(resource.table_resource, pulumi_snowflake.Table)
        self.assertIsNone(resource.database_resource)
        self.assertIsNone(resource.schema_resource)
        self.assertIsNone(resource.stage_resource)
        self.assertIsNone(resource.snow_pipe_resource)

    def test_pulumu_resources_no_type(self):
        provider = SnowflakeTableProvider(
            table="table",
            database="database",
            schema="schema",
            flush_time_secs=10,
            bucket=S3Bucket(bucket_name="bucket"),
            snow_pipe="snow_pipe",
            snowflake_stage="snowflake_stage",
            database_managed=False,
            schema_managed=False,
            snow_pipe_managed=False,
            stage_managed=False,
            account="account",
            user="user",
            private_key="private_key",
        )

        with self.assertRaises(ValueError):
            provider.pulumi_resource(
                type_=int, credentials=self.creds, opts=pulumi.ResourceOptions()
            )

    def test_pulumu_resources_non_dataclass_type(self):
        provider = SnowflakeTableProvider(
            table="table",
            database="database",
            schema="schema",
            flush_time_secs=10,
            bucket=S3Bucket(bucket_name="bucket"),
            snow_pipe="snow_pipe",
            snowflake_stage="snowflake_stage",
            database_managed=False,
            schema_managed=False,
            snow_pipe_managed=False,
            stage_managed=False,
            account="account",
            user="user",
            private_key="private_key",
        )

        with self.assertRaises(ValueError):
            provider.pulumi_resource(
                type_=int, credentials=self.creds, opts=pulumi.ResourceOptions()
            )


if __name__ == "__main__":
    unittest.main()
