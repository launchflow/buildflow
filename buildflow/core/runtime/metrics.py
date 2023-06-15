from typing import Any, Dict, Iterable
from ray.util.metrics import Gauge, Counter
from collections import deque
import time
from dataclasses import dataclass


@dataclass
class RateCalculation:
    """Stores the results of a rate calculation."""

    rate_buckets_sum: float
    num_rate_buckets: int
    # Is there a better way to model this?
    num_buckets_merge_fn: str = "sum"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "rate_buckets_sum": self.rate_buckets_sum,
            "num_rate_buckets": self.num_rate_buckets,
        }

    def rate(self) -> float:
        """Returns the average rate per bucket."""
        if self.num_rate_buckets == 0:
            return 0.0
        return self.rate_buckets_sum / self.num_rate_buckets

    @classmethod
    def merge(
        cls,
        rate_calculations: Iterable["RateCalculation"],
    ) -> "RateCalculation":
        """Merges multiple rate calculations into a single rate calculation."""
        merge_fn = None
        rate_buckets_sum = 0.0
        num_rate_buckets_sum = 0
        num_rate_buckets_count = 0
        for rate_calculation in rate_calculations:
            if merge_fn is None:
                merge_fn = rate_calculation.num_buckets_merge_fn
            elif merge_fn != rate_calculation.num_buckets_merge_fn:
                raise ValueError(
                    "Cannot merge rate calculations with different merge functions."
                )
            rate_buckets_sum += rate_calculation.rate_buckets_sum
            num_rate_buckets_sum += rate_calculation.num_rate_buckets
            num_rate_buckets_count += 1

        if num_rate_buckets_count == 0:
            return RateCalculation(0.0, 0, merge_fn)

        if merge_fn == "sum":
            num_rate_buckets = num_rate_buckets_sum
        elif merge_fn == "avg":
            num_rate_buckets = num_rate_buckets_sum / num_rate_buckets_count
        else:
            raise ValueError(f"Invalid merge_fn: {merge_fn}")

        return RateCalculation(rate_buckets_sum, num_rate_buckets, merge_fn)


# NOTE: This is only an approximation and is not meant for precise calculations.
class RateCounterMetric:
    def __init__(
        self,
        name: str,
        description: str = "",
        default_tags: Dict[str, str] = None,
        rate_secs: int = 60,
    ):
        # setup for in-memory metrics
        self.rate_secs = rate_secs
        self.buckets = deque([0.0 for _ in range(self.rate_secs)])
        self.running_count = 0
        self.running_sum = 0.0
        self.last_update = int(time.monotonic())

        # setup for ray metrics
        tag_keys = tuple(default_tags.keys()) if default_tags else None
        self._ray_counter = Counter(
            name=name, description=description, tag_keys=tag_keys
        )
        self._ray_counter.set_default_tags(default_tags)

    def inc(self, n: int = 1, *args, **kwargs):
        """Increments the counter by n and updates the rate buckets."""
        self._ray_counter.inc(n, *args, **kwargs)
        self.update_rate_buckets(n)

    def update_rate_buckets(self, value: int):
        """Updates the rate buckets with the given value."""
        now_second = int(time.monotonic())
        seconds_difference = now_second - self.last_update
        self.last_update = now_second

        # Check if more than rate_secs seconds have passed since the last update
        if seconds_difference >= self.rate_secs:
            # If so, clear all buckets
            self.buckets = deque([0.0 for _ in range(self.rate_secs)])
            self.running_count = 0
            self.running_sum = 0.0
        else:
            # Otherwise, add new buckets for the seconds that have passed
            for _ in range(seconds_difference):
                self.buckets.append(0.0)  # append a new bucket for the current second
                removed_value = self.buckets.popleft()  # remove the oldest bucket
                if removed_value:
                    self.running_count -= 1
                self.running_sum -= removed_value

        # Add the data point to the correct bucket
        self.buckets[-1] += value
        self.running_count += 1
        self.running_sum += value

    def value_per_second_rate(self) -> RateCalculation:
        """Calculates average value/sec for the sliding window."""
        # NOTE: This is supposed to mimic this prometheus query:
        # rate(my_value_counter[{rate_secs}s])
        # This is useful for metrics like avg throughput,
        return RateCalculation(
            self.running_sum, self.rate_secs, num_buckets_merge_fn="avg"
        )

    def avg_value_size_rate(self) -> RateCalculation:
        """Calculates average value size for the sliding window."""
        # NOTE: This is supposed to mimic this prometheus query:
        # rate(total_val_counter[{rate_secs}s]) / rate(num_vals_counter[{rate_secs}s])
        # This is useful for metrics like avg process time, avg batch size, etc.
        return RateCalculation(
            self.running_sum, self.running_count, num_buckets_merge_fn="sum"
        )


class SimpleGaugeMetric:
    def __init__(
        self,
        name: str,
        description: str = "",
        default_tags: Dict[str, str] = None,
    ):
        # setup for in-memory metrics
        self._latest_value = None

        # setup for ray metrics
        tag_keys = tuple(default_tags.keys()) if default_tags else None
        self._ray_gauge = Gauge(name=name, description=description, tag_keys=tag_keys)
        self._ray_gauge.set_default_tags(default_tags)

    def set(self, value: float, *args, **kwargs):
        """Sets the gauge to value."""
        self._ray_gauge.set(value, *args, **kwargs)
        self._latest_value = value

    def get(self) -> float:
        """Returns the latest value of the gauge."""
        return self._latest_value
