import logging
import unittest
from unittest import mock

from buildflow.core.app.runtime import autoscaler
from buildflow.core.app.runtime._runtime import RuntimeStatus
from buildflow.core.app.runtime.actors.consumer_pattern.consumer_pool_snapshot import (
    ConsumerProcessorGroupSnapshot,
    ConsumerProcessorSnapshot,
)
from buildflow.core.options.runtime_options import AutoscalerOptions
from buildflow.core.processor.processor import ProcessorGroupType, ProcessorType

# Set logging to debug to make test failures easier to groc.
logging.getLogger().setLevel(logging.DEBUG)


def create_snapshot(
    *,
    num_replicas: int,
    throughput: float,
    backlog: int,
    num_cpu_per_replica: float = 1,
    avg_cpu_percent: float = 1,
    timestamp_millis: int = 1,
    pull_per_sec: float = 1000,
) -> ConsumerProcessorGroupSnapshot:
    return ConsumerProcessorGroupSnapshot(
        num_replicas=num_replicas,
        num_cpu_per_replica=num_cpu_per_replica,
        status=RuntimeStatus.RUNNING,
        timestamp_millis=timestamp_millis,
        group_id="id",
        group_type=ProcessorGroupType.CONSUMER,
        num_concurrency_per_replica=1,
        processor_snapshots={
            "id": ConsumerProcessorSnapshot(
                processor_id="id",
                processor_type=ProcessorType.CONSUMER,
                source_backlog=backlog,
                total_events_processed_per_sec=throughput,
                avg_cpu_percentage_per_replica=avg_cpu_percent,
                eta_secs=1,
                total_pulls_per_sec=pull_per_sec,
                avg_num_elements_per_batch=1,
                avg_pull_percentage_per_replica=1,
                avg_process_time_millis_per_element=1,
                avg_process_time_millis_per_batch=1,
                avg_pull_to_ack_time_millis_per_batch=1,
            )
        },
    )


@mock.patch("ray.available_resources", return_value={"CPU": 32})
class ConsumerAutoScalerTest(unittest.TestCase):
    def test_scale_up_to_estimated_replicas(self, resources_mock):
        current_num_replics = 2
        current_throughput = 10
        backlog = 1000

        snapshot = create_snapshot(
            num_replicas=current_num_replics,
            throughput=current_throughput,
            backlog=backlog,
        )
        config = AutoscalerOptions(
            enable_autoscaler=True,
            min_replicas=1,
            max_replicas=100,
            num_replicas=1,
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            current_snapshot=snapshot,
            prev_snapshot=None,
            config=config,
        )
        self.assertEqual(rec_replicas, 4)

    @mock.patch("buildflow.core.app.runtime.autoscaler.request_resources")
    def test_scale_down_to_estimated_replicas(
        self, request_resources_mock: mock.MagicMock, resources_mock
    ):
        current_num_replics = 4
        current_throughput = 100000
        backlog = 0
        avg_cpu_percent = 20

        snapshot = create_snapshot(
            num_replicas=current_num_replics,
            throughput=current_throughput,
            backlog=backlog,
            avg_cpu_percent=avg_cpu_percent,
        )
        config = AutoscalerOptions(
            enable_autoscaler=True,
            min_replicas=1,
            max_replicas=100,
            num_replicas=1,
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            current_snapshot=snapshot,
            prev_snapshot=None,
            config=config,
        )
        self.assertEqual(rec_replicas, 3)
        request_resources_mock.assert_called_once_with(0)

    @mock.patch("buildflow.core.app.runtime.autoscaler.request_resources")
    def test_scale_up_when_no_pulls(
        self, request_resources_mock: mock.MagicMock, resources_mock
    ):
        current_num_replics = 4
        current_throughput = 100000
        backlog = 0
        avg_cpu_percent = 20

        snapshot = create_snapshot(
            num_replicas=current_num_replics,
            throughput=current_throughput,
            backlog=backlog,
            avg_cpu_percent=avg_cpu_percent,
            pull_per_sec=0,
        )
        config = AutoscalerOptions(
            enable_autoscaler=True,
            min_replicas=1,
            max_replicas=100,
            num_replicas=1,
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            current_snapshot=snapshot,
            prev_snapshot=None,
            config=config,
        )
        self.assertEqual(rec_replicas, 5)
        request_resources_mock.assert_not_called()

    def test_scale_up_to_max_options_replicas(self, resources_mock):
        pass

    @mock.patch("buildflow.core.app.runtime.autoscaler.request_resources")
    def test_scale_up_to_max_available_cpu_replicas(
        self, request_resources_mock: mock.MagicMock, resources_mock
    ):
        resources_mock.return_value = {"CPU": 1}
        current_num_replics = 2
        current_throughput = 10
        backlog = 1000

        snapshot = create_snapshot(
            num_replicas=current_num_replics,
            throughput=current_throughput,
            backlog=backlog,
        )
        config = AutoscalerOptions(
            enable_autoscaler=True,
            min_replicas=1,
            max_replicas=100,
            num_replicas=1,
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            current_snapshot=snapshot,
            prev_snapshot=None,
            config=config,
        )
        self.assertEqual(rec_replicas, 3)
        request_resources_mock.assert_called_once_with(num_cpus=6)

    def test_scale_up_to_max_cpu_replicas_equals_max_options(self, resources_mock):
        current_num_replics = 2
        current_throughput = 10
        backlog = 1000

        snapshot = create_snapshot(
            num_replicas=current_num_replics,
            throughput=current_throughput,
            backlog=backlog,
        )
        config = AutoscalerOptions(
            enable_autoscaler=True,
            min_replicas=1,
            max_replicas=3,
            num_replicas=1,
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            current_snapshot=snapshot,
            prev_snapshot=None,
            config=config,
        )
        self.assertEqual(rec_replicas, 3)

    @mock.patch("buildflow.core.app.runtime.autoscaler.request_resources")
    def test_scale_down_to_min_options_replicas(
        self, request_resources_mock: mock.MagicMock, resources_mock
    ):
        current_num_replics = 3
        current_throughput = 100000
        backlog = 0
        avg_cpu_percent = 5

        snapshot = create_snapshot(
            num_replicas=current_num_replics,
            throughput=current_throughput,
            backlog=backlog,
            avg_cpu_percent=avg_cpu_percent,
        )
        config = AutoscalerOptions(
            enable_autoscaler=True,
            min_replicas=2,
            max_replicas=100,
            num_replicas=3,
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            current_snapshot=snapshot,
            prev_snapshot=None,
            config=config,
        )
        self.assertEqual(rec_replicas, 2)

    def test_scale_up_to_backlog_growth(self, resources_mock):
        prev_timestamp = 1 * 1000 * 60
        prev_num_replics = 4
        prev_throughput = 4000
        prev_backlog = 50_000

        # Growth is 50,000 over 1 minute
        # So we want a throughput increase of 50,000 / 60 = 833.33
        # which will require one more replica
        current_timestamp = 2 * 1000 * 60
        current_num_replics = 4
        current_throughput = 4000
        backlog = 100_000

        prev_snapshot = create_snapshot(
            num_replicas=prev_num_replics,
            throughput=prev_throughput,
            backlog=prev_backlog,
            timestamp_millis=prev_timestamp,
        )
        current_snapshot = create_snapshot(
            num_replicas=current_num_replics,
            throughput=current_throughput,
            backlog=backlog,
            timestamp_millis=current_timestamp,
            avg_cpu_percent=100,
        )
        config = AutoscalerOptions(
            enable_autoscaler=True,
            min_replicas=1,
            max_replicas=100,
            num_replicas=1,
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            current_snapshot=current_snapshot,
            prev_snapshot=prev_snapshot,
            config=config,
        )
        self.assertEqual(rec_replicas, 5)


if __name__ == "__name__":
    unittest.main()
