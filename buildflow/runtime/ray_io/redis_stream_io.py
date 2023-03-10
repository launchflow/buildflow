"""IO connectors for Redis and Ray."""

import logging
import time
from typing import Any, Callable, Dict, Iterable, Union

import ray
import redis

from buildflow import io
from buildflow.runtime.ray_io import base


@ray.remote
class RedisStreamInput(base.RaySource):

    def __init__(
        self,
        ray_sinks: Iterable[base.RaySink],
        redis_stream_ref: io.RedisStream,
    ) -> None:
        super().__init__(ray_sinks)
        self.redis_client = redis.Redis(host=redis_stream_ref.host,
                                        port=redis_stream_ref.port)
        self.timeout_secs = redis_stream_ref.read_timeout_secs
        self.streams = {}
        for stream in redis_stream_ref.streams:
            if stream in redis_stream_ref.start_positions:
                start = redis_stream_ref.start_positions[stream]
            else:
                try:
                    stream_info = self.redis_client.xinfo_stream(stream)
                    start = stream_info['last-generated-id'].decode()
                except redis.ResponseError as e:
                    logging.info(
                        'unable to look up stream, this may occur because the '
                        'stream doesn\'t exist yet: %s', e)
                    start = 0
            self.streams[stream] = start

    async def run(self):
        logging.info(
            'Started listening to the following streams at the s'
            'pecified ID: %s', self.streams)
        start = time.time()
        while True:
            if (self.timeout_secs > 0
                    and time.time() - start > self.timeout_secs):
                break
            stream_data = self.redis_client.xread(streams=self.streams)
            for stream in stream_data:
                stream_name = stream[0]
                stream_data = stream[1]
                items = []
                for id_item in stream_data:
                    item_id, item = id_item
                    self.streams[stream_name.decode()] = item_id.decode()
                    decoded_item = {}
                    for key, value in item.items():
                        decoded_item[key.decode()] = value.decode()
                    items.append(decoded_item)
                await self._send_batch_to_sinks_and_await(items)

            time.sleep(1)


@ray.remote
class RedisStreamOutput(base.RaySink):

    def __init__(
        self,
        remote_fn: Callable,
        redis_stream_ref: io.RedisStream,
    ) -> None:
        super().__init__(remote_fn)
        self.redis_client = redis.Redis(host=redis_stream_ref.host,
                                        port=redis_stream_ref.port)
        self.streams = redis_stream_ref.streams

    async def _write(
        self,
        elements: Union[Iterable[Iterable[Dict[str, Any]]],
                        Iterable[Dict[str, Any]]],
    ):
        for stream in self.streams:

            for elem in elements:
                if isinstance(elem, dict):
                    self.redis_client.xadd(stream, elem)
                else:
                    for subelem in elem:
                        self.redis_client.xadd(stream, subelem)
