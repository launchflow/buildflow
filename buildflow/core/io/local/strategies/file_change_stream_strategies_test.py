import os
import shutil
import tempfile
import time
import unittest

import pytest

from buildflow.core.io.local.strategies.file_change_stream_strategies import (
    LocalFileChangeStreamSource,
)
from buildflow.types.local import FileChangeStreamEventType
from buildflow.types.portable import PortableFileChangeEventType


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

        # There's 4 events here cause we also get events for modifications
        self.assertEqual(len(data.payload), 4)

        create_data = data.payload[0]
        self.assertEqual(
            create_data.portable_event_type, PortableFileChangeEventType.CREATED
        )
        self.assertEqual(create_data.metadata["eventType"], "created")
        self.assertEqual(create_data.file_path, create_path)
        self.assertEqual(create_data.metadata["srcPath"], create_path)
        self.assertEqual(create_data.blob, b"hello")

        time.sleep(1)
        data = self.get_async_result(strat.pull())
        # Nothing has changed since our last pull so we should get no elements.
        self.assertEqual(len(data.payload), 0)

        with open(create_path, "a") as f:
            f.write(", world")

        time.sleep(1)
        data = self.get_async_result(strat.pull())
        # Two payloads one for modified dir and one for modified file
        self.assertEqual(len(data.payload), 2)
        modified_data = data.payload[0]
        self.assertEqual(
            modified_data.portable_event_type, PortableFileChangeEventType.UNKNOWN
        )
        self.assertEqual(modified_data.metadata["eventType"], "modified")
        self.assertEqual(modified_data.file_path, create_path)
        self.assertEqual(modified_data.metadata["srcPath"], create_path)
        self.assertEqual(modified_data.blob, b"hello, world")

        os.remove(create_path)
        time.sleep(1)
        data = self.get_async_result(strat.pull())
        # Two payloads one for modified dir and one for removed file
        self.assertEqual(len(data.payload), 2)
        modified_data = data.payload[0]
        self.assertEqual(
            modified_data.portable_event_type, PortableFileChangeEventType.DELETED
        )
        self.assertEqual(modified_data.metadata["eventType"], "deleted")
        self.assertEqual(modified_data.file_path, create_path)
        self.assertEqual(modified_data.metadata["srcPath"], create_path)
        with self.assertRaises(ValueError):
            modified_data.blob


if __name__ == "__main__":
    unittest.main()
