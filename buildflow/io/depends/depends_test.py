"""Tests for depends.py"""

from dataclasses import dataclass
import unittest

from buildflow import io


class DependsTest(unittest.TestCase):
    def test_processor_annotation_depends(self):
        source = io.GCPPubSubSubscription(
            topic_id="projects/p/topics/topic", subscription_id="projects/p/topics/sub"
        )

        pubsub_depends = io.Depends(source)

        self.assertIsInstance(pubsub_depends, io.Push)

    def test_unsupported_source_depends_no_provider(self):
        source = "not a provider"

        with self.assertRaises(io.UnsupportDepenendsSource):
            io.Depends(source)

    def test_unsupported_source_depends_no_push_provider_as_method(self):
        @dataclass
        class _Temp:
            a: str

            def provider(self):
                return self.a

        with self.assertRaises(io.UnsupportDepenendsSource):
            io.Depends(_Temp(1))

    def test_unsupported_source_depends_no_push_provider_as_arg(self):
        @dataclass
        class _Temp:
            provider: str

        with self.assertRaises(io.UnsupportDepenendsSource):
            io.Depends(_Temp(1))


if __name__ == "__main__":
    unittest.main()
