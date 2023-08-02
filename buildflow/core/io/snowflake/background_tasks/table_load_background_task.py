import asyncio
import datetime
import logging
import os
from typing import Any, Dict, Union

import fsspec
import ray
from snowflake.ingest import SimpleIngestManager, StagedFile

from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.io.snowflake.constants import (
    BASE_STAGING_DIR,
    BASE_UPLOAD_DIR,
    UPLOAD_ACTOR_NAME,
)
from buildflow.core.io.utils.file_systems import get_file_system


class SnowflakeUploadBackgroundTask(BackgroundTask):
    def __init__(
        self,
        credentials: Union[AWSCredentials, GCPCredentials],
        bucket_name: str,
        account: str,
        user: str,
        database: str,
        schema: str,
        pipe: str,
        private_key: str,
        flush_time_secs: int,
    ):
        self.bucket_name = bucket_name
        self.file_system = get_file_system(credentials)
        self.flush_actor = None
        self.flush_loop = None
        self.account = account
        self.user = user
        self.database = database
        self.schema = schema
        self.pipe = pipe
        self.private_key = private_key
        self.flush_time_secs = flush_time_secs

    async def start(self):
        self.flush_actor = _SnowflakeUploadActor.options(name=UPLOAD_ACTOR_NAME).remote(
            bucket_name=self.bucket_name,
            file_system=self.file_system,
            account=self.account,
            user=self.user,
            database=self.database,
            schema=self.schema,
            pipe=self.pipe,
            private_key=self.private_key,
            flush_time_secs=self.flush_time_secs,
        )
        self.flush_loop = self.flush_actor.flush.remote()

    async def shutdown(self):
        if self.flush_actor is not None:
            logging.info(
                "Shutting down SnowflakeUploadActor will stop after next flush"
            )
            await self.flush_actor.shutdown.remote()
            await self.flush_loop


@ray.remote(max_restarts=-1, num_cpus=0.1)
class _SnowflakeUploadActor:
    def __init__(
        self,
        bucket_name: str,
        file_system: fsspec.AbstractFileSystem,
        account: str,
        user: str,
        database: str,
        schema: str,
        pipe: str,
        private_key: str,
        flush_time_secs: int,
    ):
        self.bucket_name = bucket_name
        self.file_system = file_system
        self.staging_dir = os.path.join(self.bucket_name, BASE_STAGING_DIR)
        self.upload_dir = os.path.join(self.bucket_name, BASE_UPLOAD_DIR)
        self.running = True
        pipe = f'"{database}"."{schema}"."{pipe}"'
        self.ingest_manager = SimpleIngestManager(
            account=account, user=user, pipe=pipe, private_key=private_key
        )
        self.flush_time_secs = flush_time_secs
        self.staged_files = []

    def mv_file(self, src_path: str, dest_path: str) -> str:
        self.file_system.mv(src_path, dest_path)
        return dest_path

    async def mv_files(self):
        loop = asyncio.get_event_loop()
        try:
            files: Dict[str, Any] = await loop.run_in_executor(
                None, self.file_system.ls, self.staging_dir, True
            )
        except FileNotFoundError:
            files = []
        coros = []
        staged_files = []
        for file in files:
            src_file_path = file["name"]
            src_file_size = file["size"]
            upload_file_path = os.path.join(
                self.upload_dir, os.path.basename(src_file_path)
            )

            def mv_file():
                self.file_system.mv(src_file_path, upload_file_path)
                return upload_file_path, src_file_size

            coros.append(loop.run_in_executor(None, mv_file))
        if coros:
            done, _ = await asyncio.wait(coros)
            for completed in done:
                try:
                    file_path, file_size = completed.result()
                    # Trim the bucket name from the file path since the file system
                    # expects
                    # it but snowflake wants it relative to the URL.
                    staged_files.append(
                        StagedFile(
                            file_path.removeprefix(f"{self.bucket_name}/"), file_size
                        )
                    )
                except Exception:
                    logging.exception(
                        "Failed to move file will keep in staging dir and retry on next"
                        " flush."
                    )
        try:
            if staged_files:
                datetime.datetime.utcnow().isoformat()
                response = self.ingest_manager.ingest_files(staged_files)
                if response["responseCode"] != "SUCCESS":
                    logging.error(
                        "Failed to ingest files to Snowflake: %s", response["message"]
                    )
                    return
                # while True:
                #     history = self.ingest_manager.get_history_range(
                #         start_time_inclusive=ingest_start
                #     )

        except Exception:
            logging.exception("Failed to upload files to Snowflake")
            return

    async def flush(self):
        while self.running:
            await asyncio.sleep(self.flush_time_secs)
            await self.mv_files()

    async def shutdown(self):
        self.running = False
