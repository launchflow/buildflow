from typing import Any, Dict, Callable, Iterable
from ray.util.metrics import Gauge, Counter
from collections import deque
import time
from dataclasses import dataclass


@dataclass
class RateCalculation:
    """Stores the results of a rate calculation."""

    rate_buckets_sum: float
    num_rate_buckets: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "rate_buckets_sum": self.rate_buckets_sum,
            "num_rate_buckets": self.num_rate_buckets,
        }


AggregationFn = Callable[[Iterable[Any]], float]


# NOTE: This class is supposed to mimic this prometheus query:
# avg_over_time({aggregation_fn}_over_time(counter_name)[1s])[{rate_sec}]
# where the final average result is stored as: (rate_buckets_sum, num_rate_buckets)
# so we can combine results together before calculating the final average.
# i.e.: average_across_replicas = sum(rate_buckets_sum) / sum(num_rate_buckets)
class RateMetric:
    def __init__(self, rate_secs: int, aggregation_fn: AggregationFn):
        self.rate_secs = rate_secs
        self.aggregation_fn = aggregation_fn
        # Each entry is a list that will hold data points for a given second
        self.buckets = deque([[] for _ in range(self.rate_secs)])
        self.last_update = int(time.time())

    def update_rate_buckets(self, value: int):
        now_second = int(time.time())
        seconds_difference = now_second - self.last_update
        self.last_update = now_second

        # Check if more than rate_secs seconds have passed since the last update
        if seconds_difference >= self.rate_secs:
            # If so, clear all buckets
            self.buckets = deque([[] for _ in range(self.rate_secs)])
        else:
            # Otherwise, add new buckets for the seconds that have passed
            for _ in range(int(seconds_difference)):
                self.buckets.append([])  # append a new bucket for the current second
                self.buckets.popleft()  # remove the oldest bucket

        # Add the data point to the correct bucket
        self.buckets[-1].append(value)

    def calculate(self) -> RateCalculation:
        """Calculates the average rate per bucket within the last rate_secs."""
        now_second = int(time.time())
        seconds_difference = now_second - self.last_update

        if seconds_difference >= self.rate_secs:
            return RateCalculation(0.0, 0)

        # Calculate the sum of all buckets in the last rate_secs
        rate_buckets_sum = 0.0
        num_rate_buckets = 0
        for bucket in self.buckets:
            if bucket:
                rate_buckets_sum += self.aggregation_fn(bucket)
            num_rate_buckets += 1

        return RateCalculation(rate_buckets_sum, num_rate_buckets)


# NOTE: This class is supposed to mimic this prometheus query:
# avg_over_time(sum_over_time(counter_name)[1s])[{rate_sec}]
class RateCounterMetric(RateMetric):
    def __init__(
        self,
        name: str,
        description: str = "",
        default_tags: Dict[str, str] = None,
        rate_secs: int = 60,
    ):
        super().__init__(rate_secs, aggregation_fn=sum)
        self.name = name
        # Ray Metrics API
        tag_keys = tuple(default_tags.keys()) if default_tags else None
        self._ray_counter = Counter(
            name=self.name, description=description, tag_keys=tag_keys
        )
        self._ray_counter.set_default_tags(default_tags)

    def inc(self, n: int = 1, *args, **kwargs):
        """Increments the counter by n and updates the rate buckets."""
        self._ray_counter.inc(n, *args, **kwargs)
        self.update_rate_buckets(n)


def avg(x):
    return sum(x) / len(x)


# NOTE: This class is supposed to mimic this prometheus query:
# avg_over_time(avg_over_time(gauge_name)[1s])[{rate_sec}]
# which I think is mostly the same as:
# avg_over_time(rate(gauge_name)[1s])[{rate_sec}]
# except that prometheus rate is supposed to be used with counters, not gauges.
# https://prometheus.io/docs/prometheus/latest/querying/functions/#rate
class RateGaugeMetric(RateMetric):
    def __init__(
        self,
        name: str,
        description: str = "",
        default_tags: Dict[str, str] = None,
        rate_secs: int = 60,
    ):
        super().__init__(rate_secs, aggregation_fn=avg)
        self.name = name
        # Ray Metrics API
        tag_keys = tuple(default_tags.keys()) if default_tags else None
        self._ray_gauge = Gauge(
            name=self.name, description=description, tag_keys=tag_keys
        )
        self._ray_gauge.set_default_tags(default_tags)

    def set(self, value: float, *args, **kwargs):
        """Sets the gauge to value and updates the rate buckets."""
        self._ray_gauge.set(value, *args, **kwargs)
        self.update_rate_buckets(value)


class SimpleGaugeMetric:
    def __init__(
        self,
        name: str,
        description: str = "",
        default_tags: Dict[str, str] = None,
    ):
        self.name = name
        # Ray Metrics API
        tag_keys = tuple(default_tags.keys()) if default_tags else None
        self._ray_gauge = Gauge(
            name=self.name, description=description, tag_keys=tag_keys
        )
        self._ray_gauge.set_default_tags(default_tags)
        self._latest_value = None

    def set(self, value: float, *args, **kwargs):
        """Sets the gauge to value."""
        self._ray_gauge.set(value, *args, **kwargs)
        self._latest_value = value

    def get(self) -> float:
        """Returns the latest value of the gauge."""
        return self._latest_value
