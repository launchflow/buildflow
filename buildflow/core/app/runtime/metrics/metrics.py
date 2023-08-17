import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Union

from ray.util.metrics import Counter, Gauge


@dataclass
class RateCalculation:
    """Stores the results of a rate calculation."""

    values_sum: float
    values_count: int
    num_rate_seconds: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "values_sum": self.values_sum,
            "values_count": self.values_count,
            "num_rate_seconds": self.num_rate_seconds,
        }

    # NOTE: This is supposed to mimic this prometheus query:
    # rate(value_ray_counter[{rate_secs}s]) / rate(count_ray_counter[{rate_secs}s])
    # This is used by "average VALUE per rate window" metrics like average batch size
    def average_value_rate(self) -> float:
        """Calculates the average rate of the sampled values."""
        if self.values_count == 0:
            return 0.0
        return self.values_sum / self.values_count

    # NOTE: This is supposed to mimic this prometheus query:
    # rate(value_ray_counter[{rate_secs}s])
    # This is used by "total VALUE per sec" metrics like throughput (QPS)
    def total_value_rate(self) -> float:
        """Calculates the total rate of the samples values."""
        if self.num_rate_seconds == 0:
            return 0.0
        return self.values_sum / self.num_rate_seconds

    # NOTE: total_value_rate is essentially the same as total_count_rate, but we need it
    # because some "composite" metrics are used to track multiple things at once (e.g.
    # 'pull_percentage' metric inside the PullProcessPushActor uses total_count_rate()
    # to calculate the total rate of pulls, and the average_value_rate() to calculate
    # the average percentage of successful pulls)

    # NOTE: This is supposed to mimic this prometheus query:
    # rate(count_ray_counter[{rate_secs}s])
    # This is used by "average COUNT per sec" metrics like throughput (QPS)
    def total_count_rate(self) -> float:
        """Calculates the total rate of counts."""
        if self.num_rate_seconds == 0:
            return 0.0
        return self.values_count / self.num_rate_seconds

    @classmethod
    def merge(
        cls,
        rate_calculations: Iterable["RateCalculation"],
    ) -> "RateCalculation":
        """Calculates the average rate using multiple rate calculations."""
        combined_values_sum = 0.0
        combined_values_count = 0
        combined_num_rate_seconds = 0
        num_rate_calculations = 0
        for rate_calculation in rate_calculations:
            combined_values_sum += rate_calculation.values_sum
            combined_values_count += rate_calculation.values_count
            combined_num_rate_seconds += rate_calculation.num_rate_seconds
            num_rate_calculations += 1

        if combined_num_rate_seconds == 0:
            # This case should only happen when no rate calculations were provided
            return cls(0.0, 0, 0)

        average_num_rate_seconds = combined_num_rate_seconds / num_rate_calculations

        return cls(combined_values_sum, combined_values_count, average_num_rate_seconds)


# NOTE: This is only an approximation and is not meant for precise calculations.
class CompositeRateCounterMetric:
    """A composite of 2 Counter metrics that do in-memory rate calculations."""

    def __init__(
        self,
        name: str,
        description: str = "",
        default_tags: Dict[str, str] = None,
        rate_secs: int = 60,
    ):
        # setup for in-memory metrics
        self.rate_secs = rate_secs
        self.buckets = deque([(0, 0) for _ in range(self.rate_secs)])
        self.running_count = 0
        self.running_sum = 0.0
        self.running_time_secs = 0
        self.last_update_sec = int(time.monotonic())

        # setup for ray metrics
        tag_keys = tuple(default_tags.keys()) if default_tags else None
        self._count_ray_counter = Counter(
            name=f"{name}_count", description=description, tag_keys=tag_keys
        )
        self._count_ray_counter.set_default_tags(default_tags)
        self._value_ray_counter = Counter(
            name=f"{name}_sum", description=description, tag_keys=tag_keys
        )
        self._value_ray_counter.set_default_tags(default_tags)

    def inc(self, n: Union[int, float] = 1, *args, **kwargs):
        """Increments both the COUNT & VALUE counters and updates the rate buckets."""
        self._count_ray_counter.inc(1, *args, **kwargs)
        self._value_ray_counter.inc(n, *args, **kwargs)
        self.update_rate_buckets(n)

    def empty_inc(self, *args, **kwargs):
        """Only increments the COUNT counter and updates the rate buckets."""
        self._count_ray_counter.inc(1, *args, **kwargs)
        self.update_rate_buckets(0)

    def update_rate_buckets(self, value: Union[int, float]):
        """Updates the rate buckets with the given value."""
        now_sec = int(time.monotonic())
        seconds_difference = now_sec - self.last_update_sec
        self.last_update_sec = now_sec
        self.running_time_secs += seconds_difference

        # Check if more than rate_secs seconds have passed since the last update
        if seconds_difference >= self.rate_secs:
            # If so, clear all buckets
            self.buckets = deque([(0, 0) for _ in range(self.rate_secs)])
            self.running_count = 0
            self.running_sum = 0.0
        else:
            # Otherwise, add new buckets for the seconds that have passed
            for _ in range(seconds_difference):
                # append a new bucket for the current second
                self.buckets.append((0, 0))
                # remove the oldest bucket
                removed_count, removed_sum = self.buckets.popleft()
                if removed_count > 0:
                    self.running_count -= removed_count
                    self.running_sum -= removed_sum

        # Add the data point to the correct bucket
        current_count, current_sum = self.buckets[-1]
        self.buckets[-1] = (current_count + 1, current_sum + value)
        self.running_count += 1
        self.running_sum += value

    def calculate_rate(self) -> RateCalculation:
        """Calculates the rate using the latest running counters."""
        # Handles the case where less than rate_secs has passed
        rate_secs = min(self.running_time_secs, self.rate_secs)
        return RateCalculation(self.running_sum, self.running_count, rate_secs)


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

    def get_latest_value(self) -> float:
        """Returns the latest value of the gauge."""
        if self._latest_value is None:
            return 0.0
        return self._latest_value
