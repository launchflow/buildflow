import time
import unittest
from unittest import mock

import pytest

from buildflow.io.local.pulse import Pulse


@pytest.mark.usefixtures("event_loop_instance")
class PulsingProviderTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    def test_pulsing_provider(self):
        pulse = Pulse(items=[1, 2, 3], pulse_interval_seconds=1)
        pulse_source = pulse.source_provider().source(mock.MagicMock())

        start_time = time.time()
        for i in range(3):
            result = self.get_async_result(pulse_source.pull())
            self.assertEqual(result.payload, [i + 1])
        end_time = time.time() - start_time
        self.assertGreaterEqual(end_time, 3)


if __name__ == "__main__":
    unittest.main()
