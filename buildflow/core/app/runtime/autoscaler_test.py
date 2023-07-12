import logging
import unittest
from unittest import mock

import pytest

from buildflow.core.app.runtime import autoscaler
from buildflow.core.app.runtime._runtime import RuntimeStatus
from buildflow.core.options.runtime_options import AutoscalerOptions
from buildflow.core.app.runtime.actors.pipeline_pattern.pipeline_pool import (
    PipelineProcessorSnapshot,
)
from buildflow.core.processor.processor import ProcessorType


# Set logging to debug to make test failures easier to groc.
logging.getLogger().setLevel(logging.DEBUG)


def create_snapshot(
    *,
    num_replicas: int,
    throughput: float,
    backlog: int,
    num_cpu_per_replica: float = 1,
    avg_cpu_percent: float = 1,
) -> PipelineProcessorSnapshot:
    return PipelineProcessorSnapshot(
        source_backlog=backlog,
        total_events_processed_per_sec=throughput,
        num_replicas=num_replicas,
        avg_cpu_percentage_per_replica=avg_cpu_percent,
        num_cpu_per_replica=num_cpu_per_replica,
        status=RuntimeStatus.RUNNING,
        timestamp_millis=1,
        processor_id="id",
        processor_type=ProcessorType.PIPELINE,
        num_concurrency_per_replica=1,
        eta_secs=1,
        total_pulls_per_sec=1,
        avg_num_elements_per_batch=1,
        avg_pull_percentage_per_replica=1,
        avg_process_time_millis_per_element=1,
        avg_process_time_millis_per_batch=1,
        avg_pull_to_ack_time_millis_per_batch=1,
    )


@mock.patch("ray.available_resources", return_value={"CPU": 32})
class PipelineAutoScalerTest(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

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
            enable_autoscaler=True, min_replicas=1, max_replicas=100, log_level="UNUSED"
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            snapshot,
            config,
        )
        self.assertEqual(rec_replicas, 4)

    @mock.patch("buildflow.core.app.runtime.autoscaler.request_resources")
    def test_scale_down_to_estimated_replicas(
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
            enable_autoscaler=True, min_replicas=1, max_replicas=100, log_level="UNUSED"
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            snapshot,
            config,
        )
        self.assertEqual(rec_replicas, 1)
        request_resources_mock.assert_called_once_with(num_cpus=1)

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
            enable_autoscaler=True, min_replicas=1, max_replicas=100, log_level="UNUSED"
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            snapshot,
            config,
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
            enable_autoscaler=True, min_replicas=1, max_replicas=3, log_level="UNUSED"
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            snapshot,
            config,
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
            enable_autoscaler=True, min_replicas=2, max_replicas=100, log_level="UNUSED"
        )
        logging
        rec_replicas = autoscaler.calculate_target_num_replicas(
            snapshot,
            config,
        )
        self.assertEqual(rec_replicas, 2)


if __name__ == "__name__":
    unittest.main()
