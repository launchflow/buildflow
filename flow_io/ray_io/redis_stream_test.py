"""Tests for redis.py"""

import json
import os
import tempfile
import time
import unittest

import ray
from ray.dag.input_node import InputNode

from flow_io.ray_io import redis_stream


class FakeRedisClient():

    def __init__(self, stream_file: str) -> None:
        self.stream_file = stream_file
        self.stream_data = {}
        self.stream_ids = {}

    def _load_file(self):
        with open(self.stream_file, 'r', encoding='UTF-8') as f:
            try:
                data = json.load(f)
                return data['stream_data'], data['stream_ids']
            except json.JSONDecodeError:
                return {}, {}

    def _write_file(self, stream_data, stream_ids):
        data = {
            'stream_data': stream_data,
            'stream_ids': stream_ids,
        }
        with open(self.stream_file, 'w', encoding='UTF-8') as f:
            json.dump(data, f)

    def xadd(self, stream, data):
        stream_data, stream_ids = self._load_file()
        if stream not in stream_data:
            stream_ids[stream] = 1
            stream_data[stream] = {stream_ids[stream]: data}
        else:
            stream_data[stream][stream_ids[stream]] = data
        stream_ids[stream] = stream_ids[stream] + 1
        self._write_file(stream_data, stream_ids)

    def xread(self, streams):
        stream_data, _ = self._load_file()
        to_ret = []
        for stream, id_data in stream_data.items():
            if stream not in streams:
                continue
            stream_data = []
            for id, data in id_data.items():
                if int(id) < streams[stream]:
                    continue
                encoded_data = {}
                for key, value in data.items():
                    encoded_data[key.encode()] = value.encode()
                stream_data.append((str(id).encode(), encoded_data))
            to_ret.append([stream.encode(), stream_data])
        return to_ret


class RedisStream(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_end_to_end(self):

        @ray.remote
        def ray_func(input):
            return input

        input_config = {
            'streams': ['input_stream'],
            'host': 'host',
            'port': 'str',
            'start_positions': {
                'input_stream': 0
            }
        }

        output_config = {
            'streams': ['output_stream'],
            'host': 'host',
            'port': 'str',
        }

        _, tmp_redis_file = tempfile.mkstemp()

        r = FakeRedisClient(tmp_redis_file)

        r.xadd('input_stream', {'field': 'value'})
        expected = [[
            'output_stream'.encode(),
            [('1'.encode(), {
                'field'.encode(): 'value'.encode()
            })]
        ]]

        try:
            with InputNode() as input:
                ray_func_ref = ray_func.bind(input)
                output_ref = redis_stream.RedisStreamOutput.bind(
                    **output_config, redis_client=r)
                output = output_ref.write.bind(ray_func_ref)

            redis_stream.RedisStreamInput([output],
                                          **input_config,
                                          num_reads=1,
                                          redis_client=r).run()

            time.sleep(10)
            data_written = r.xread({'output_stream': 0})
            self.assertEqual(expected, data_written)
        finally:
            os.remove(tmp_redis_file)


if __name__ == '__main__':
    unittest.main()
