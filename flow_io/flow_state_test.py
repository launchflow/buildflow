import unittest

import flow_io


class FlowStateTest(unittest.TestCase):

    def test_new_flow_state(self):
        flow_io.init()


if __name__ == '__main__':
    unittest.main()
