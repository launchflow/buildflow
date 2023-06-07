import logging
import unittest
from unittest import mock

import pytest

from buildflow.core.runtime import autoscale
from buildflow.core.runtime.actors.process_pool import (
    ProcessorSnapshot,
    RayActorInfo,
    ReplicaSnapshot,
    SourceInfo,
)
from buildflow.core.runtime.config import AutoscalerConfig


@mock.patch("ray.available_resources", return_value={"CPU": 32})
class AutoScalerTest(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    def test_scale_up_to_estimated_replicas(self, resources_mock):
        num_replicas = 3
        backlog = 100_000
        # 12_000 events per minute means we can burn down a backlog 12_000
        # in one minute so our estimated number of replicas would be 16
        # (100_000 / 12_00).
        events_processed_per_replica = 12_000
        non_empty_ratio_per_replica = 1
        replicas = [
            ReplicaSnapshot(
                utilization_score=non_empty_ratio_per_replica,
                process_rate=events_processed_per_replica,
            )
        ] * num_replicas
        with self._caplog.at_level(logging.WARNING):
            rec_replicas = autoscale.calculate_target_num_replicas(
                snapshot=ProcessorSnapshot(
                    processor_id="test",
                    source=SourceInfo(backlog=backlog, provider=None),
                    sink=None,
                    replicas=replicas,
                    actor_info=RayActorInfo(num_cpus=0.1),
                ),
                config=AutoscalerConfig(
                    enable_autoscaler=True,
                    min_replicas=1,
                    max_replicas=10000,
                ),
            )
            self.assertEqual(len(self._caplog.records), 1)
            expected_log = "resizing from 3 replicas to 8 replicas"
            self.assertEqual(self._caplog.records[0].message, expected_log)

        self.assertEqual(8, rec_replicas)

    @mock.patch("buildflow.core.runtime.autoscale.request_resources")
    def test_scale_down_to_estimated_replicas(
        self, request_resources_mock: mock.MagicMock, resources_mock
    ):
        num_replicas = 24
        backlog = 100_000
        # 15_000 events per minute means we can burn down a backlog 15_000
        # in minute so our estimated number of replicas would be 6
        # (100_000 / 15_000).
        events_processed_per_replica = 15_000
        # Our non empty ratio is low meaning we have low utilization so we will
        # attempt to scale down. With this ratio we would scale down to 2
        # replica, but based on the backlog we will keep 8 replicas to ensure
        # the backlog can be burned down.
        non_empty_ratio_per_replica = 0.1
        replicas = [
            ReplicaSnapshot(
                utilization_score=non_empty_ratio_per_replica,
                process_rate=events_processed_per_replica,
            )
        ] * num_replicas
        with self._caplog.at_level(logging.WARNING):
            rec_replicas = autoscale.calculate_target_num_replicas(
                snapshot=ProcessorSnapshot(
                    processor_id="test",
                    source=SourceInfo(backlog=backlog, provider=None),
                    sink=None,
                    replicas=replicas,
                    actor_info=RayActorInfo(num_cpus=0.1),
                ),
                config=AutoscalerConfig(
                    enable_autoscaler=True,
                    min_replicas=1,
                    max_replicas=10000,
                ),
            )
            self.assertEqual(len(self._caplog.records), 1)
            expected_log = "resizing from 24 replicas to 6 replicas"
            self.assertEqual(self._caplog.records[0].message, expected_log)

        self.assertEqual(6, rec_replicas)
        # .1 * 6 = .6 so 1 CPU
        request_resources_mock.assert_called_once_with(num_cpus=1)

    def test_scale_up_to_max_options_replicas(self, resources_mock):
        num_replicas = 3
        backlog = 100_000
        # 12_000 events per minute means we can burn down a backlog 12_000
        # in minute so our estimated number of replicas would be 8
        # (100_000 / 12_00).
        events_processed_per_replica = 12_000
        non_empty_ratio_per_replica = 1
        replicas = [
            ReplicaSnapshot(
                utilization_score=non_empty_ratio_per_replica,
                process_rate=events_processed_per_replica,
            )
        ] * num_replicas
        with self._caplog.at_level(logging.WARNING):
            rec_replicas = autoscale.calculate_target_num_replicas(
                snapshot=ProcessorSnapshot(
                    processor_id="test",
                    source=SourceInfo(backlog=backlog, provider=None),
                    sink=None,
                    replicas=replicas,
                    actor_info=RayActorInfo(num_cpus=0.1),
                ),
                config=AutoscalerConfig(
                    enable_autoscaler=True,
                    min_replicas=1,
                    max_replicas=5,
                ),
            )
            self.assertEqual(len(self._caplog.records), 1)
            expected_log = "resizing from 3 replicas to 5 replicas"
            self.assertEqual(self._caplog.records[0].message, expected_log)

        self.assertEqual(5, rec_replicas)

    @mock.patch("buildflow.core.runtime.autoscale.request_resources")
    def test_scale_up_to_max_cpu_replicas(
        self, request_resources_mock: mock.MagicMock, resources_mock
    ):
        num_replicas = 3
        backlog = 100_000
        # 100 events per minute means we can burn down a backlog 100
        # in minute so our estimated number of replicas would be 1000
        # (100_000 / 100).
        # However this will be limited by the number of available CPUs on
        # our cluster. We have 32 available CPUs in our mock cluster,
        # so we will only scale to:
        #   32 / .5 = 67 (num replicas + num available replics (3 + 64))
        events_processed_per_replica = 100
        non_empty_ratio_per_replica = 1
        replicas = [
            ReplicaSnapshot(
                utilization_score=non_empty_ratio_per_replica,
                process_rate=events_processed_per_replica,
            )
        ] * num_replicas
        with self._caplog.at_level(logging.WARNING):
            rec_replicas = autoscale.calculate_target_num_replicas(
                snapshot=ProcessorSnapshot(
                    processor_id="test",
                    source=SourceInfo(backlog=backlog, provider=None),
                    sink=None,
                    replicas=replicas,
                    actor_info=RayActorInfo(num_cpus=0.5),
                ),
                config=AutoscalerConfig(
                    enable_autoscaler=True,
                    min_replicas=1,
                    max_replicas=1000,
                ),
            )
            expected_log = "resizing from 3 replicas to 67 replicas"
            self.assertEqual(self._caplog.records[0].message, expected_log)

            self.assertEqual(67, rec_replicas)
        # This is capped to only request double of what we're scaling to.
        # This is equal to our number of replicas because our replica per
        # CPU is .5
        request_resources_mock.assert_called_once_with(num_cpus=67)

    def test_scale_up_to_max_cpu_replicas_equals_max_options(self, resources_mock):
        num_replicas = 3
        backlog = 100_000
        # 100 events per minute means we can burn down a backlog 100
        # in minute so our estimated number of replicas would be 1000
        # (100_000 / 100).
        # However this will be limited by the number of CPUs on our cluster.
        # We have 32 CPUs in our mock cluster, so we will only scale to:
        #   32 / .25 * .66 = 84
        #   num_cpus / source_cpus / REPLICA_CPU_RATION
        events_processed_per_replica = 100
        non_empty_ratio_per_replica = 1
        replicas = [
            ReplicaSnapshot(
                utilization_score=non_empty_ratio_per_replica,
                process_rate=events_processed_per_replica,
            )
        ] * num_replicas
        with self._caplog.at_level(logging.WARNING):
            rec_replicas = autoscale.calculate_target_num_replicas(
                snapshot=ProcessorSnapshot(
                    processor_id="test",
                    source=SourceInfo(backlog=backlog, provider=None),
                    sink=None,
                    replicas=replicas,
                    actor_info=RayActorInfo(num_cpus=0.1),
                ),
                config=AutoscalerConfig(
                    enable_autoscaler=True,
                    min_replicas=1,
                    max_replicas=84,
                ),
            )
            self.assertEqual(len(self._caplog.records), 1)
            expected_log = "resizing from 3 replicas to 84 replicas"
            self.assertEqual(self._caplog.records[0].message, expected_log)

            self.assertEqual(84, rec_replicas)

    @mock.patch("buildflow.core.runtime.autoscale.request_resources")
    def test_scale_down_to_target_utilization(
        self, request_resources_mock: mock.MagicMock, resources_mock
    ):
        num_replicas = 24
        backlog = 10
        # 15_000 events per minute means we can burn down a backlog 15_000
        # in minute so our estimated number of replicas would be 6
        # (10 / 15_000).
        events_processed_per_replica = 15_000
        # Our non empty ratio is low meaning we have low utilization so we will
        # attempt to scale down. With this ratio we would scale down to 15
        # replicas to achieve a utilization of .5
        non_empty_ratio_per_replica = 0.3
        replicas = [
            ReplicaSnapshot(
                utilization_score=non_empty_ratio_per_replica,
                process_rate=events_processed_per_replica,
            )
        ] * num_replicas
        with self._caplog.at_level(logging.WARNING):
            rec_replicas = autoscale.calculate_target_num_replicas(
                snapshot=ProcessorSnapshot(
                    processor_id="test",
                    source=SourceInfo(backlog=backlog, provider=None),
                    sink=None,
                    replicas=replicas,
                    actor_info=RayActorInfo(num_cpus=0.1),
                ),
                config=AutoscalerConfig(
                    enable_autoscaler=True,
                    min_replicas=1,
                    max_replicas=1000,
                ),
            )
            self.assertEqual(len(self._caplog.records), 1)
            expected_log = "resizing from 24 replicas to 15 replicas"
            self.assertEqual(self._caplog.records[0].message, expected_log)

        self.assertEqual(15, rec_replicas)
        # .1 * 15 = 1.5 so 2 cpus
        request_resources_mock.assert_called_once_with(num_cpus=2)

    @mock.patch("buildflow.core.runtime.autoscale.request_resources")
    def test_scale_down_to_min_options_replicas(
        self, request_resources_mock: mock.MagicMock, resources_mock
    ):
        num_replicas = 24
        backlog = 10
        # 15_000 events per minute means we can burn down a backlog 15_000
        # in minute so our estimated number of replicas would be 6
        # (10 / 15_000).
        events_processed_per_replica = 15_000
        # Our non empty ratio is low meaning we have low utilization so we will
        # attempt to scale down. With this ratio we would scale down to 6
        # replicas to achieve a utilization of .75
        non_empty_ratio_per_replica = 0.3

        replicas = [
            ReplicaSnapshot(
                utilization_score=non_empty_ratio_per_replica,
                process_rate=events_processed_per_replica,
            )
        ] * num_replicas
        with self._caplog.at_level(logging.WARNING):
            rec_replicas = autoscale.calculate_target_num_replicas(
                snapshot=ProcessorSnapshot(
                    processor_id="test",
                    source=SourceInfo(backlog=backlog, provider=None),
                    sink=None,
                    replicas=replicas,
                    actor_info=RayActorInfo(num_cpus=0.1),
                ),
                config=AutoscalerConfig(
                    enable_autoscaler=True,
                    min_replicas=18,
                    max_replicas=1000,
                ),
            )
            self.assertEqual(len(self._caplog.records), 1)
            expected_log = "resizing from 24 replicas to 18 replicas"
            self.assertEqual(self._caplog.records[0].message, expected_log)

        self.assertEqual(18, rec_replicas)
        # .1 * 18 = 1.8 so 2 cpus
        request_resources_mock.assert_called_once_with(num_cpus=2)


if __name__ == "__name__":
    unittest.main()
