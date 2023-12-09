import asyncio
import os
import shutil
import tempfile
import unittest

from buildflow.io.local.strategies.file_change_stream_strategies import (
    LocalFileChangeStreamSource,
)
from buildflow.types.local import FileChangeStreamEventType


class FileChangeStreamStrategiesTest(unittest.TestCase):
    def setUp(self):
        self.watch_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.watch_dir)

    async def test_all_file_change_event_types(self):
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
            data = await strat.pull()
            await asyncio.sleep(1)
            create_path = os.path.join(self.watch_dir, "file.txt")
            with open(create_path, "w") as f:
                f.write("hello")

            await asyncio.sleep(1)

            data = await strat.pull()

            self.assertGreaterEqual(len(data.payload), 1)
            found_file_create = False
            for event in data.payload:
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

            await asyncio.sleep(1)
            data = await strat.pull()
            # Nothing has changed since our last pull so we should get no elements.
            self.assertEqual(len(data.payload), 0)

            os.remove(create_path)
            await asyncio.sleep(1)
            data = await strat.pull()
            self.assertGreaterEqual(len(data.payload), 1)
            found_delete_event = False
            for event in data.payload:
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
            await strat.teardown()


if __name__ == "__main__":
    unittest.main()
