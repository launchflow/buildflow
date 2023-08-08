import os
import shutil
import tempfile
import time
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

        create_path = os.path.join(self.watch_dir, "file.txt")
        with open(create_path, "w") as f:
            f.write("hello")

        time.sleep(1)
        data = self.get_async_result(strat.pull())

        # There's at least 3 events here cause we also get events for modifications
        self.assertGreaterEqual(len(data.payload), 3)
        found_file_create = False
        for event in data.payload:
            if (
                event.event_type == FileChangeStreamEventType.CREATED
                and not event.metadata["isDirectory"]
            ):
                found_file_create = True
                self.assertEqual(event.metadata["eventType"], "created")
                # NOTE: we do "in" checks here because on OSX temp files
                # get created in /var/... which is actualy a symlink to /private/var/...
                self.assertIn(create_path, event.file_path)
                self.assertIn(create_path, event.metadata["srcPath"])
                self.assertEqual(event.blob, b"hello")

        if not found_file_create:
            self.fail("Did not find file create event")

        time.sleep(1)
        data = self.get_async_result(strat.pull())
        # Nothing has changed since our last pull so we should get no elements.
        self.assertEqual(len(data.payload), 0)

        with open(create_path, "a") as f:
            f.write(", world")

        time.sleep(3)
        data = self.get_async_result(strat.pull())
        # On linux two payloads one for modified dir and one for modified file
        # On mac one payload for modified file
        self.assertGreaterEqual(len(data.payload), 1)

        found_modify_event = False
        for event in data.payload:
            if (
                event.event_type == FileChangeStreamEventType.MODIFIED
                and not event.metadata["isDirectory"]
            ):
                found_modify_event = True
                self.assertEqual(event.metadata["eventType"], "modified")
                # NOTE: we do "in" checks here because on OSX temp files
                # get created in /var/... which is actualy a symlink to /private/var/...
                self.assertIn(create_path, event.file_path)
                self.assertIn(create_path, event.metadata["srcPath"])
                self.assertEqual(event.blob, b"hello, world")
        if not found_modify_event:
            self.fail("Did not find file modify event")

        os.remove(create_path)
        time.sleep(1)
        data = self.get_async_result(strat.pull())
        # On linux two payloads one for modified dir and one for removed file
        # On linux three payloads
        #   - one for modified dir
        #   - one for modified file
        #   - one for removed file
        self.assertGreaterEqual(len(data.payload), 2)
        found_delete_event = False
        for event in data.payload:
            if (
                event.event_type == FileChangeStreamEventType.DELETED
                and not event.metadata["isDirectory"]
            ):
                found_delete_event = True
                self.assertEqual(event.metadata["eventType"], "deleted")
                # NOTE: we do "in" checks here because on OSX temp files
                # get created in /var/... which is actualy a symlink to /private/var/...
                self.assertIn(create_path, event.file_path)
                self.assertIn(create_path, event.metadata["srcPath"])
                with self.assertRaises(ValueError):
                    event.blob
        if not found_delete_event:
            self.fail("Did not find file delete event")


if __name__ == "__main__":
    unittest.main()
