import asyncio
import datetime
import logging
import os
from typing import Any, Dict, Optional, Union

import fsspec
import ray
from snowflake.ingest import SimpleIngestManager, StagedFile

from buildflow.core.background_tasks.background_task import BackgroundTask
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.io.snowflake.constants import (
    BASE_STAGING_DIR,
    BASE_UPLOAD_DIR,
    UPLOAD_ACTOR_NAME,
)
from buildflow.io.utils.file_systems import get_file_system

_MAX_HISTORY_CHECK_TIME_SECS = 120


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
        test_ingest_manager: Optional[Any] = None,
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
        self.test_ingest_manager = test_ingest_manager

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
            test_ingest_manager=self.test_ingest_manager,
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
        test_ingest_manager: Optional[Any] = None,
    ):
        self.bucket_name = bucket_name
        self.file_system = file_system
        self.staging_dir = os.path.join(self.bucket_name, BASE_STAGING_DIR)
        self.upload_dir = os.path.join(self.bucket_name, BASE_UPLOAD_DIR)
        self.running = True
        pipe = f'"{database}"."{schema}"."{pipe}"'
        if test_ingest_manager is not None:
            self.ingest_manager = test_ingest_manager
        else:
            self.ingest_manager = SimpleIngestManager(
                account=account, user=user, pipe=pipe, private_key=private_key
            )
        self.flush_time_secs = flush_time_secs
        self.staged_files = []

    def mv_file(self, src_path: str, dest_path: str) -> str:
        self.file_system.mv(src_path, dest_path)
        return dest_path

    def ls_files(self, src_path: str) -> Dict[str, Any]:
        # NOTE: We include refresh=True here to ensure we are always
        # getting the latest files from the bucket.
        return self.file_system.ls(src_path, True, refresh=True)

    async def mv_files(self):
        loop = asyncio.get_event_loop()
        files = []
        try:
            files: Dict[str, Any] = await loop.run_in_executor(
                None, self.ls_files, self.staging_dir
            )
        except FileNotFoundError:
            # This happens when the staging dir doesn't exist yet.
            # Meaning there are no files to upload
            return
        except Exception:
            logging.exception("Failed to list files in staging dir")
            return
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
                ingest_start = (
                    datetime.datetime.utcnow()
                    .replace(tzinfo=datetime.timezone.utc)
                    .isoformat()
                )
                response = self.ingest_manager.ingest_files(staged_files)
                if response["responseCode"] != "SUCCESS":
                    logging.error(
                        "Failed to ingest files to Snowflake: %s", response["message"]
                    )
                    return

        except Exception:
            logging.exception("Failed to upload files to Snowflake")
            return
        history_loop_start = datetime.datetime.utcnow()
        staged_file_paths = {file.path for file in staged_files}
        while True:
            if not staged_file_paths:
                break
            if (
                history_loop_start - datetime.datetime.utcnow()
            ).total_seconds() > _MAX_HISTORY_CHECK_TIME_SECS:
                break
            history_resp = self.ingest_manager.get_history_range(
                start_time_inclusive=ingest_start
            )
            ingest_start = history_resp["rangeEndTime"]
            if len(history_resp.get("files", [])) <= 0:
                # wait ten seconds and search for more
                await asyncio.sleep(10)
                continue
            for file in history_resp["files"]:
                if "path" not in file or file["status"] == "LOAD_IN_PROGRESS":
                    continue
                path = file["path"]
                if path in staged_file_paths:
                    staged_file_paths.remove(path)
                    if file["errorsSeen"] > 0:
                        logging.error("failed to load file to snowflake: ", file)

        if staged_file_paths:
            logging.error(
                "Failed to find all files in Snowflake history after %s seconds. "
                "Files: %s",
                _MAX_HISTORY_CHECK_TIME_SECS,
                staged_file_paths,
            )
            return

    async def flush(self):
        while self.running:
            await asyncio.sleep(self.flush_time_secs)
            try:
                await self.mv_files()
            except Exception:
                logging.exception("Failed to flush files to Snowflake")

    async def shutdown(self):
        self.running = False
