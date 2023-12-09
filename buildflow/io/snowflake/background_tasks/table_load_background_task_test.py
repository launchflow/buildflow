import asyncio
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

import pytest

from buildflow.core.credentials.empty_credentials import EmptyCredentials
from buildflow.io.snowflake.background_tasks.table_load_background_task import (
    SnowflakeUploadBackgroundTask,
)
from buildflow.io.snowflake.constants import BASE_STAGING_DIR, BASE_UPLOAD_DIR


@pytest.mark.usefixtures("ray")
class TableLoadBackgroundTaskTest(unittest.IsolatedAsyncioTestCase):
    async def run_for_time(self, coro, time: int = 5):
        completed, pending = await asyncio.wait(
            [coro], timeout=time, return_when="FIRST_EXCEPTION"
        )
        if completed:
            # This general should only happen when there was an exception so
            # we want to raise it to make the test failure more obvious.
            completed.pop().result()
        if pending:
            return pending.pop()

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.staging_dir = os.path.join(self.temp_dir, BASE_STAGING_DIR)
        os.mkdir(self.staging_dir)
        self.upload_dir = os.path.join(self.temp_dir, BASE_UPLOAD_DIR)
        os.mkdir(self.upload_dir)
        self.background_task = SnowflakeUploadBackgroundTask(
            # We use empty credentials so we use the local file system
            credentials=EmptyCredentials(),
            bucket_name=self.temp_dir,
            account="account",
            user="user",
            database="database",
            schema="schema",
            pipe="pipe",
            private_key="pk",
            flush_time_secs=1,
            test_ingest_manager=mock.MagicMock(),
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    async def test_upload_base(self):
        # write some test data
        to_upload_file_path = os.path.join(self.staging_dir, "file1.json")
        with open(to_upload_file_path, "w") as f:
            json.dump({"test": "data"}, f)

        # run the background tasks
        await self.background_task.start()
        await self.run_for_time(self.background_task.flush_loop, time=10)

        # check that the file was uploaded
        self.assertIn("file1.json", os.listdir(self.upload_dir))


if __name__ == "__main__":
    unittest.main()
