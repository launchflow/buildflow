import asyncio
import enum
import os
from typing import Any, Callable, Dict, Type, Union

import fsspec
import ray

from buildflow.core.background_tasks.background_task import (
    BackgroundTask,
    BackgroundTaskLifeCycle,
)
from buildflow.core.credentials.aws_credentials import AWSCredentials
from buildflow.core.credentials.gcp_credentials import GCPCredentials
from buildflow.core.io.aws.strategies.s3_strategies import S3BucketSink
from buildflow.core.io.gcp.strategies.storage_strategies import GCSBucketSink
from buildflow.core.strategies.sink import Batch, SinkStrategy
from buildflow.core.utils import uuid


class _BucketType(enum.Enum):
    S3 = 1
    GCS = 2


_BASE_STAGING_DIR = "buildflow-staging"
_BASE_UPLOAD_DIR = "buildflow-upload"
_UPLOAD_ACTOR_NAME = "SnowflakeUploadActor"


class SnowflakeTableSink(SinkStrategy):
    def __init__(
        self,
        credentials: Union[AWSCredentials, GCPCredentials],
        # Bucket for staging data for upload to snowflake
        bucket_sink: Union[S3BucketSink, GCSBucketSink],
    ):
        super().__init__(credentials, "snowflake-table-sink")
        self.credentials = credentials
        self.bucket_sink = bucket_sink
        self.bucket_sink.file_path = self._get_new_file_path()
        self.background_task_started = False

    def _get_new_file_path(self) -> str:
        return os.path.join(
            self.bucket_sink.bucket_name, _BASE_STAGING_DIR, f"{uuid()}.parquet"
        )

    def push_converter(
        self, user_defined_type: Type
    ) -> Callable[[Any], Dict[str, Any]]:
        return self.bucket_sink.push_converter(user_defined_type)

    async def push(self, batch: Batch):
        await self.bucket_sink.push(batch)
        self.bucket_sink.file_path = self._get_new_file_path()

    def background_tasks(self):
        return [
            _SnowflakeUploadBackgroundTask(
                self.bucket_sink.bucket_name, self.bucket_sink.file_system
            )
        ]


class _SnowflakeUploadBackgroundTask(BackgroundTask):
    # TODO: need to find a way to have this attached to the processor's life cycle
    # instead of just the replica
    # with ideally only one of these background tasks being created
    def __init__(self, bucket_name: str, file_system: fsspec.AbstractFileSystem):
        self.bucket_name = bucket_name
        self.file_system = file_system
        self.flush_actor = None
        self.flush_loop = None

    def lifecycle(self) -> BackgroundTaskLifeCycle:
        return BackgroundTaskLifeCycle.PROCESSOR

    async def start(self):
        try:
            ray.get_actor(_UPLOAD_ACTOR_NAME)
            # Actor exists already, no need to create/start it again
            return
        except ValueError:
            pass
        self.flush_actor = _SnowflakeUploadActor.options(
            name=_UPLOAD_ACTOR_NAME
        ).remote(bucket_name=self.bucket_name, file_system=self.file_system)
        self.flush_loop = self.flush_actor.flush.remote()

    async def shutdown(self):
        if self.flush_actor is not None:
            await self.flush_actor.shutdown.remote()
            await self.flush_loop


@ray.remote(max_restarts=-1, num_cpus=0.1)
class _SnowflakeUploadActor:
    def __init__(self, bucket_name: str, file_system: fsspec.AbstractFileSystem):
        self.bucket_name = bucket_name
        self.file_system = file_system
        self.staging_dir = os.path.join(self.bucket_name, _BASE_STAGING_DIR)
        self.upload_dir = os.path.join(self.bucket_name, _BASE_UPLOAD_DIR)
        self.running = True

    async def mv_files(self):
        loop = asyncio.get_event_loop()
        try:
            files = await loop.run_in_executor(
                None, self.file_system.ls, self.staging_dir
            )
        except FileNotFoundError:
            files = []
        coros = []
        for file in files:
            coros.append(
                loop.run_in_executor(
                    None,
                    self.file_system.mv,
                    file,
                    os.path.join(self.upload_dir, os.path.basename(file)),
                ),
            )
        await asyncio.gather(*coros)

    async def flush(self):
        while self.running:
            await asyncio.sleep(60)
            await self.mv_files()

    async def shutdown(self):
        self.running = False
