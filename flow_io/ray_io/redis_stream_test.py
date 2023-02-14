"""Tests for redis.py"""

import json
import os
import shutil
import tempfile
import unittest

import ray

from flow_io import ray_io


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
            for r_id, data in id_data.items():
                if int(r_id) < int(streams[stream]):
                    continue
                encoded_data = {}
                for key, value in data.items():
                    encoded_data[key.encode()] = value.encode()
                stream_data.append((str(r_id).encode(), encoded_data))
            to_ret.append([stream.encode(), stream_data])
        return to_ret


class RedisStream(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=1, num_gpus=0)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def setUp(self) -> None:
        _, self.temp_file = tempfile.mkstemp()
        self.temp_dir = tempfile.mkdtemp()
        self.flow_file = os.path.join(self.temp_dir, 'flow_state.json')
        self.deployment_file = os.path.join(self.temp_dir, 'deployment.json')
        os.environ['FLOW_FILE'] = self.flow_file
        os.environ['FLOW_DEPLOYMENT_FILE'] = self.deployment_file
        with open(self.flow_file, 'w', encoding='UTF-8') as f:
            flow_contents = {
                'nodes': [
                    {
                        'nodeSpace': 'flow_io/ray_io'
                    },
                    {
                        'nodeSpace': 'resource/queue/redis/stream/stream1',
                    },
                    {
                        'nodeSpace': 'resource/queue/redis/stream/stream2',
                    },
                ],
                'outgoingEdges': {
                    'resource/queue/redis/stream/stream1': ['flow_io/ray_io'],
                    'flow_io/ray_io': ['resource/queue/redis/stream/stream2'],
                },
            }
            json.dump(flow_contents, f)
        with open(self.deployment_file, 'w', encoding='UTF-8') as f:
            json.dump(
                {
                    'nodeDeployments': {
                        'resource/queue/redis/stream/stream1': {
                            'host': 'localhost',
                            'port': '6879',
                            'streams': ['input_stream'],
                            'start_positions': {
                                'input_stream': 0
                            },
                        },
                        'resource/queue/redis/stream/stream2': {
                            'host': 'localhost',
                            'port': '6879',
                            'streams': ['output_stream']
                        }
                    }
                }, f)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_end_to_end(self):

        r = FakeRedisClient(self.temp_file)

        r.xadd('input_stream', {'field': 'value'})
        expected = [[
            'output_stream'.encode(),
            [('1'.encode(), {
                'field'.encode(): 'value'.encode()
            })]
        ]]

        sink = ray_io.sink(redis_client=r)
        source = ray_io.source(sink.write, redis_client=r, num_reads=1)
        ray.get(source.run.remote())

        data_written = r.xread({'output_stream': 0})
        self.assertEqual(expected, data_written)

    def test_end_to_end_multi_output(self):

        r = FakeRedisClient(self.temp_file)

        r.xadd('input_stream', {'field': 'value'})
        expected = [[
            'output_stream'.encode(),
            [('1'.encode(), {
                'field'.encode(): 'value'.encode()
            }), ('2'.encode(), {
                'field'.encode(): 'value'.encode()
            })]
        ]]

        @ray.remote
        class ProcessActor:

            def __init__(self, sink):
                self.sink = sink

            def process(self, elem, carrier):
                return ray.get(sink.write.remote([elem, elem], carrier))

        sink = ray_io.sink(redis_client=r)
        processor = ProcessActor.remote(sink)
        source = ray_io.source(processor.process, redis_client=r, num_reads=1)
        ray.get(source.run.remote())

        data_written = r.xread({'output_stream': 0})
        self.assertEqual(expected, data_written)


if __name__ == '__main__':
    unittest.main()
