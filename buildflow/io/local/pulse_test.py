import time
import unittest
from unittest import mock

from buildflow.io.local.pulse import Pulse


class PulsePrimitiveTest(unittest.IsolatedAsyncioTestCase):
    async def test_pulsing_source(self):
        pulse = Pulse(items=[1, 2, 3], pulse_interval_seconds=1)
        pulse_source = pulse.source(mock.MagicMock())

        start_time = time.time()
        for i in range(3):
            result = await pulse_source.pull()
            self.assertEqual(result.payload, [i + 1])
        end_time = time.time() - start_time
        self.assertGreaterEqual(end_time, 3)


if __name__ == "__main__":
    unittest.main()
