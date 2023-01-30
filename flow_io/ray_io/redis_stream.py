"""IO connectors for Redis and Ray."""

import logging
import time
from typing import Any, Dict, Iterable, List, Optional

import ray
import redis


def _get_redis_client(redis_config: Dict[str, Any]) -> redis.Redis:
    return redis.Redis(host=redis_config['host'], port=redis_config['port'])


class RedisStreamInput:

    def __init__(
        self,
        ray_dags: Iterable,
        host: str,
        port: int,
        streams: List[str],
        start_positions: Optional[Dict[str, str]] = None,
        # Number of reads to do from redis.
        # If this is <= 0 it will continually pull data.
        num_reads: int = -1,
        # Redis client to use. In general this should only be set for
        # testing purposes.
        redis_client=None,
    ) -> None:
        if redis_client is None:
            redis_client = redis.Redis(host=host, port=port)
        self.redis_client = redis_client
        self.ray_dags = ray_dags
        self.redis_client = redis_client
        self.num_reads = num_reads
        self.streams = {}
        if start_positions is None:
            start_positions = {}
        for stream in streams:
            if stream in start_positions:
                start = start_positions[stream]
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

    def run(self):
        i = 0
        logging.info(
            'Started listening to the following streams at the s'
            'pecified ID: %s', self.streams)
        while i < self.num_reads or self.num_reads <= 0:
            i += 1
            stream_data = self.redis_client.xread(streams=self.streams)
            for stream in stream_data:
                stream_name = stream[0]
                stream_data = stream[1]
                for id_item in stream_data:
                    item_id, item = id_item
                    self.streams[stream_name.decode()] = item_id.decode()
                    decoded_item = {}
                    for key, value in item.items():
                        decoded_item[key.decode()] = value.decode()
                    for ray_dag in self.ray_dags:
                        ray_dag.execute(decoded_item)

            time.sleep(1)


@ray.remote
class RedisStreamOutput:

    def __init__(
        self,
        host: str,
        port: int,
        streams: List[str],
        start_positions: Optional[Dict[str, str]] = None,
        redis_client=None,
    ) -> None:
        if redis_client is None:
            redis_client = redis.Redis(host=host, port=port)
        self.redis_client = redis_client
        self.streams = streams

    def write(self, element: Dict):
        for stream in self.streams:
            self.redis_client.xadd(stream, element)
