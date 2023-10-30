import asyncio
import os
import shutil
import tempfile
import unittest

import pytest

from buildflow.io.local.strategies.file_change_stream_strategies import (
    LocalFileChangeStreamSource,
)
from buildflow.types.local import FileChangeStreamEventType


@pytest.mark.usefixtures("event_loop_instance")
class FileChangeStreamStrategiesTest(unittest.TestCase):
    def get_async_result(self, coro):
        """Run a coroutine synchronously."""
        return self.event_loop.run_until_complete(coro)

    def setUp(self):
        self.watch_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.watch_dir)

    def test_all_file_change_event_types(self):
        strat = LocalFileChangeStreamSource(
            credentials=None,
            file_path=self.watch_dir,
            event_types=(
                FileChangeStreamEventType.CREATED,
                FileChangeStreamEventType.DELETED,
                FileChangeStreamEventType.MODIFIED,
            ),
        )

        try:
            data = self.get_async_result(strat.pull())
            self.get_async_result(asyncio.sleep(1))
            create_path = os.path.join(self.watch_dir, "file.txt")
            with open(create_path, "w") as f:
                f.write("hello")

            self.get_async_result(asyncio.sleep(1))

            data = self.get_async_result(strat.pull())

            self.assertGreaterEqual(len(data.payloads), 1)
            found_file_create = False
            for event, _ in data.payloads:
                if (
                    event.event_type == FileChangeStreamEventType.CREATED
                    and not os.path.isdir(event.file_path)
                ):
                    found_file_create = True
                    self.assertEqual(
                        event.event_type, FileChangeStreamEventType.CREATED
                    )
                    # NOTE: we do "in" checks here because on OSX temp files
                    # get created in /var/... which is actualy a symlink to
                    # /private/var/...
                    self.assertIn(self.watch_dir, event.file_path)
                    self.assertEqual(event.blob, b"hello")

            if not found_file_create:
                self.fail("Did not find file create event")

            self.get_async_result(asyncio.sleep(1))
            data = self.get_async_result(strat.pull())
            # Nothing has changed since our last pull so we should get no elements.
            self.assertEqual(len(data.payloads), 0)

            os.remove(create_path)
            self.get_async_result(asyncio.sleep(1))
            data = self.get_async_result(strat.pull())
            self.assertGreaterEqual(len(data.payloads), 1)
            found_delete_event = False
            for event, _ in data.payloads:
                if (
                    event.event_type == FileChangeStreamEventType.DELETED
                    and not os.path.isdir(event.file_path)
                ):
                    found_delete_event = True
                    self.assertEqual(
                        event.event_type, FileChangeStreamEventType.DELETED
                    )
                    # NOTE: we do "in" checks here because on OSX temp files
                    # get created in /var/... which is actualy a symlink to
                    # /private/var/...
                    self.assertIn(self.watch_dir, event.file_path)
                    with self.assertRaises(ValueError):
                        event.blob
            if not found_delete_event:
                self.fail("Did not find file delete event")
        finally:
            self.get_async_result(strat.teardown())


if __name__ == "__main__":
    unittest.main()
