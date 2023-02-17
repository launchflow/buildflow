"""Tests for redis.py"""

import redis
import subprocess
import unittest

import ray

from flow_io import ray_io
import flow_io


class RedisStream(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.redis_process = subprocess.Popen(
            'redis-server --port 8765'.split(' '))
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()
        cls.redis_process.kill()

    def test_end_to_end(self):

        redis_client = redis.Redis(host='localhost', port=8765)

        redis_client.xadd('input_stream', {'field': 'value'})

        flow_io.init(
            config={
                'input':
                flow_io.RedisStream(
                    'localhost',
                    8765,
                    streams=['input_stream'],
                    start_positions={'input_stream': 0},
                    read_timeout_secs=5,
                ),
                'outputs': [
                    flow_io.RedisStream(
                        'localhost', 8765, streams=['output_stream'])
                ]
            })

        @ray.remote
        def process(element):
            return element

        ray_io.run(process.remote)

        data_written = redis_client.xread({'output_stream': 0})
        self.assertEqual(len(data_written), 1)
        self.assertEqual(len(data_written[0]), 2)
        self.assertEqual(data_written[0][1][0][1],
                         {'field'.encode(): 'value'.encode()})

    def test_end_to_end_multi_output(self):

        redis_client = redis.Redis(host='localhost', port=8765)

        redis_client.xadd('input_stream', {'field': 'value'})

        flow_io.init(
            config={
                'input':
                flow_io.RedisStream(
                    'localhost',
                    8765,
                    streams=['input_stream'],
                    start_positions={'input_stream': 0},
                    read_timeout_secs=5,
                ),
                'outputs': [
                    flow_io.RedisStream(
                        'localhost', 8765, streams=['output_stream'])
                ]
            })

        @ray.remote
        def process(element):
            return element

        ray_io.run(process.remote)

        data_written = redis_client.xread({'output_stream': 0})
        self.assertEqual(data_written[0][1][0][1],
                         {'field'.encode(): 'value'.encode()})
        self.assertEqual(data_written[0][1][1][1],
                         {'field'.encode(): 'value'.encode()})


if __name__ == '__main__':
    unittest.main()
