import unittest

import pytest


@pytest.mark.usefixtures("event_loop_instance")
class AsyncTestCase(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)
