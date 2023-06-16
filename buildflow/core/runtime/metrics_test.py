import logging
import unittest
from unittest import mock

import pytest
import time

from buildflow.api import RuntimeStatus
from buildflow.core.runtime import autoscale
from buildflow.core.runtime.actors.process_pool import (
    ProcessorSnapshot,
    PullProcessPushSnapshot,
    RayActorInfo,
    SourceInfo,
)
from buildflow.core.runtime.config import AutoscalerConfig
from buildflow.core.runtime.metrics import RateCalculation, CompositeRateCounterMetric


class MetricsTest(unittest.TestCase):
    def test_composite_rate_counter(self):
        # starting buckets = [(0, 0), (0, 0), (0, 0), (0, 0), (0, 0)]
        counter = CompositeRateCounterMetric("test", "desc", {}, rate_secs=5)
        counter.inc(10)
        result = counter.calculate_rate()
        # expected buckets = [(0, 0), (0, 0), (0, 0), (0, 0), (1, 10)]
        self.assertEqual(
            result, RateCalculation(values_sum=10, values_count=1, num_rate_seconds=5)
        )
        time.sleep(1)
        counter.inc(20)
        result = counter.calculate_rate()
        # expected buckets = [(0, 0), (0, 0), (0, 0), (1, 10), (1, 20)]
        self.assertEqual(
            result, RateCalculation(values_sum=30, values_count=2, num_rate_seconds=5)
        )
        time.sleep(1)
        counter.inc(10)
        result = counter.calculate_rate()
        # expected buckets = [(0, 0), (0, 0), (1, 10), (1, 20), (1, 10)]
        self.assertEqual(
            result, RateCalculation(values_sum=40, values_count=3, num_rate_seconds=5)
        )
        time.sleep(1)
        counter.inc(20)
        counter.inc(30)
        result = counter.calculate_rate()
        # expected buckets = [(0, 0), (1, 10), (1, 20), (1, 10), (2, 50)]
        self.assertEqual(
            result, RateCalculation(values_sum=90, values_count=5, num_rate_seconds=5)
        )
        time.sleep(3)
        counter.inc(20)
        result = counter.calculate_rate()
        # NOTE: a 2 seconds were skipped since we slept for 3 seconds
        # expected buckets = [(1, 10), (2, 50), (0, 0), (0, 0), (1, 20)]
        self.assertEqual(
            result, RateCalculation(values_sum=80, values_count=4, num_rate_seconds=5)
        )

        # counts sum / rate_secs
        expected_total_count_rate = 4 / 5
        self.assertEqual(result.total_count_rate(), expected_total_count_rate)
        # values sum / rate_secs
        expected_total_value_rate = 80 / 5
        self.assertEqual(result.total_value_rate(), expected_total_value_rate)
        # values sum / values count
        expected_average_value_rate = 80 / 4
        self.assertEqual(result.average_value_rate(), expected_average_value_rate)


if __name__ == "__name__":
    unittest.main()
