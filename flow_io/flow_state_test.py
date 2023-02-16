import os
import json
import tempfile
import unittest

import flow_io


class FlowStateTest(unittest.TestCase):

    def tearDown(self) -> None:
        del os.environ[flow_io.FLOW_STATE_ENV_VAR_NAME]

    def test_new_flow_state(self):
        expected_file = {
            'node_states': {
                __file__: {
                    'input_ref': {
                        'topic': 'my_type',
                        'subscriber': '',
                        '_io_type': 'PUBSUB'
                    },
                    'output_refs': [{
                        'project': '',
                        'dataset': '',
                        'table': '',
                        'query': 'SELECT * FROM asdf',
                        '_io_type': 'BIG_QUERY'
                    }]
                }
            }
        }
        flow_io.init(
            config={
                'input': flow_io.PubSub(topic='my_type'),
                'outputs': [flow_io.BigQuery(query='SELECT * FROM asdf')]
            })

        with open(os.environ[flow_io.FLOW_STATE_ENV_VAR_NAME]) as f:
            got = json.load(f)
        self.assertEqual(got, expected_file)

    def test_append_flow_state(self):
        expected = {
            'node_states': {
                'existing_entry_point.py': {
                    'input_ref': {
                        'topic': 'my_type',
                        'subscriber': '',
                        '_io_type': 'PUBSUB'
                    },
                    'output_refs': [{
                        'project': '',
                        'dataset': '',
                        'table': '',
                        'query': 'SELECT * FROM asdf',
                        '_io_type': 'BIG_QUERY'
                    }]
                },
                __file__: {
                    'input_ref': {
                        'topic': 'my_type',
                        'subscriber': '',
                        '_io_type': 'PUBSUB'
                    },
                    'output_refs': [{
                        'project': '',
                        'dataset': '',
                        'table': '',
                        'query': 'SELECT * FROM asdf',
                        '_io_type': 'BIG_QUERY'
                    }]
                }
            }
        }
        existing_file = {
            'node_states': {
                'existing_entry_point.py': {
                    'input_ref': {
                        'topic': 'my_type',
                        'subscriber': '',
                        '_io_type': 'PUBSUB'
                    },
                    'output_refs': [{
                        'project': '',
                        'dataset': '',
                        'table': '',
                        'query': 'SELECT * FROM asdf',
                        '_io_type': 'BIG_QUERY'
                    }]
                }
            }
        }

        _, temp = tempfile.mkstemp(suffix='.json')
        with open(temp, 'w') as f:
            json.dump(existing_file, f)
        os.environ[flow_io.FLOW_STATE_ENV_VAR_NAME] = temp

        flow_io.init(
            config={
                'input': flow_io.PubSub(topic='my_type'),
                'outputs': [flow_io.BigQuery(query='SELECT * FROM asdf')]
            })

        with open(os.environ[flow_io.FLOW_STATE_ENV_VAR_NAME]) as f:
            got = json.load(f)
        self.assertEqual(got, expected)

    def test_update_existing_flow_state(self):
        expected = {
            'node_states': {
                __file__: {
                    'input_ref': {
                        'topic': 'my_type',
                        'subscriber': '',
                        '_io_type': 'PUBSUB'
                    },
                    'output_refs': [{
                        'project': '',
                        'dataset': '',
                        'table': '',
                        'query': 'SELECT * FROM asdf',
                        '_io_type': 'BIG_QUERY'
                    }]
                }
            }
        }
        existing_file = {
            'node_states': {
                __file__: {
                    'input_ref': {
                        'topic': 'my_type',
                        'subscriber': '',
                        '_io_type': 'PUBSUB'
                    },
                    'output_refs': [{
                        'project': '',
                        'dataset': '',
                        'table': '',
                        'query': 'SELECT * FROM fdsa',
                        '_io_type': 'BIG_QUERY'
                    }]
                }
            }
        }

        _, temp = tempfile.mkstemp(suffix='.json')
        with open(temp, 'w') as f:
            json.dump(existing_file, f)
        os.environ[flow_io.FLOW_STATE_ENV_VAR_NAME] = temp

        flow_io.init(
            config={
                'input': flow_io.PubSub(topic='my_type'),
                'outputs': [flow_io.BigQuery(query='SELECT * FROM asdf')]
            })

        with open(os.environ[flow_io.FLOW_STATE_ENV_VAR_NAME]) as f:
            got = json.load(f)
            print('DO NOT SUBMIT: ', got)
        self.assertEqual(got, expected)


if __name__ == '__main__':
    unittest.main()
