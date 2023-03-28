"""Tests for redis.py"""

import dataclasses
import redis
import subprocess
import time
import unittest

import pytest

import buildflow


@dataclasses.dataclass
class Output:
    field: str


@pytest.mark.usefixtures('ray_fix')
class RedisStreamTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.redis_process = subprocess.Popen(
            'redis-server --port 8765'.split(' '))
        time.sleep(2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.redis_process.kill()

    def setUp(self) -> None:
        self.flow = buildflow.Flow()

    def test_end_to_end_redis(self):

        redis_client = redis.Redis(host='localhost', port=8765)

        redis_client.xadd('input_stream', {'field': 'value'})

        @self.flow.processor(
            source=buildflow.RedisStreamSource(
                host='localhost',
                port=8765,
                streams=['input_stream'],
                start_positions={'input_stream': 0},
            ),
            sink=buildflow.RedisStreamSink(host='localhost',
                                           port=8765,
                                           streams=['output_stream']),
        )
        def process(element):
            return element

        runner = self.flow.run(streaming_options=buildflow.StreamingOptions(
            autoscaling=False))
        time.sleep(10)

        data_written = redis_client.xread({'output_stream': 0})
        self.assertEqual(len(data_written), 1)
        self.assertEqual(len(data_written[0]), 2)
        self.assertEqual(data_written[0][1][0][1],
                         {'field'.encode(): 'value'.encode()})

        runner.shutdown()

    def test_end_to_end_multi_output(self):

        redis_client = redis.Redis(host='localhost', port=8765)

        redis_client.xadd('input_stream', {'field': 'value'})

        @self.flow.processor(
            source=buildflow.RedisStreamSource(
                host='localhost',
                port=8765,
                streams=['input_stream'],
                start_positions={'input_stream': 0},
                read_timeout_secs=5,
            ),
            sink=buildflow.RedisStreamSink(host='localhost',
                                           port=8765,
                                           streams=['output_stream']),
        )
        def process(element):
            return [element, element]

        runner = self.flow.run(streaming_options=buildflow.StreamingOptions(
            autoscaling=False))
        time.sleep(10)

        data_written = redis_client.xread({'output_stream': 0})
        self.assertEqual(data_written[0][1][0][1],
                         {'field'.encode(): 'value'.encode()})
        self.assertEqual(data_written[0][1][1][1],
                         {'field'.encode(): 'value'.encode()})

        runner.shutdown()

    def test_end_to_end_dataclass(self):

        redis_client = redis.Redis(host='localhost', port=8765)

        redis_client.xadd('input_stream', {'field': 'value'})

        @self.flow.processor(
            source=buildflow.RedisStreamSource(
                host='localhost',
                port=8765,
                streams=['input_stream'],
                start_positions={'input_stream': 0},
                read_timeout_secs=5,
            ),
            sink=buildflow.RedisStreamSink(host='localhost',
                                           port=8765,
                                           streams=['output_stream']),
        )
        def process(element):
            return Output(element['field'])

        runner = self.flow.run(streaming_options=buildflow.StreamingOptions(
            autoscaling=False))
        time.sleep(10)

        data_written = redis_client.xread({'output_stream': 0})
        self.assertEqual(len(data_written), 1)
        self.assertEqual(len(data_written[0]), 2)
        self.assertEqual(data_written[0][1][0][1],
                         {'field'.encode(): 'value'.encode()})

        runner.shutdown()

    def test_end_to_end_multi_output_dataclass(self):

        redis_client = redis.Redis(host='localhost', port=8765)

        redis_client.xadd('input_stream', {'field': 'value'})

        @self.flow.processor(
            source=buildflow.RedisStreamSource(
                host='localhost',
                port=8765,
                streams=['input_stream'],
                start_positions={'input_stream': 0},
                read_timeout_secs=5,
            ),
            sink=buildflow.RedisStreamSink(host='localhost',
                                           port=8765,
                                           streams=['output_stream']),
        )
        def process(element):
            return [Output(element['field']), Output(element['field'])]

        runner = self.flow.run(streaming_options=buildflow.StreamingOptions(
            autoscaling=False))
        time.sleep(10)

        data_written = redis_client.xread({'output_stream': 0})
        self.assertEqual(data_written[0][1][0][1],
                         {'field'.encode(): 'value'.encode()})
        self.assertEqual(data_written[0][1][1][1],
                         {'field'.encode(): 'value'.encode()})
        runner.shutdown()


if __name__ == '__main__':
    unittest.main()
