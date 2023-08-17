import time
import unittest

from buildflow.core.app.runtime.metrics import (
    CompositeRateCounterMetric,
    RateCalculation,
)


class MetricsTest(unittest.TestCase):
    def test_composite_rate_counter(self):
        # starting buckets = [(0, 0), (0, 0), (0, 0), (0, 0), (0, 0)]
        counter = CompositeRateCounterMetric("test", "desc", {}, rate_secs=5)
        time.sleep(1)
        counter.inc(10)
        result = counter.calculate_rate()
        # expected buckets = [(0, 0), (0, 0), (0, 0), (0, 0), (1, 10)]
        self.assertEqual(
            result, RateCalculation(values_sum=10, values_count=1, num_rate_seconds=1)
        )
        time.sleep(1)
        counter.empty_inc()
        result = counter.calculate_rate()
        # NOTE: empty inc() does not update the values_sum
        # expected buckets = [(0, 0), (0, 0), (0, 0), (1, 10), (1, 0)]
        self.assertEqual(
            result, RateCalculation(values_sum=10, values_count=2, num_rate_seconds=2)
        )
        time.sleep(1)
        counter.inc(10)
        result = counter.calculate_rate()
        # expected buckets = [(0, 0), (0, 0), (1, 10), (1, 0), (1, 10)]
        self.assertEqual(
            result, RateCalculation(values_sum=20, values_count=3, num_rate_seconds=3)
        )
        time.sleep(0.99)
        counter.inc(20)
        counter.inc(30)
        result = counter.calculate_rate()
        # expected buckets = [(0, 0), (1, 10), (1, 0), (1, 10), (2, 50)]
        self.assertEqual(
            result, RateCalculation(values_sum=70, values_count=5, num_rate_seconds=4)
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
