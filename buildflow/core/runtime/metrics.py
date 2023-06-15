from typing import Any, Dict, Iterable, Union
from ray.util.metrics import Gauge, Counter
from collections import deque
import time
from dataclasses import dataclass


@dataclass
class RateCalculation:
    """Stores the results of a rate calculation."""

    sum_of_samples: float
    num_samples: int
    num_rate_seconds: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "sum_of_samples": self.sum_of_samples,
            "num_samples": self.num_samples,
            "num_rate_seconds": self.num_rate_seconds,
        }

    # NOTE: This is supposed to mimic this prometheus query:
    # rate(total_val_counter[{rate_secs}s]) / rate(num_vals_counter[{rate_secs}s])
    # This is used by "average Rate per replica" metrics like process time & batch size
    def average_rate(self) -> float:
        """Calculates the average rate."""
        if self.num_samples == 0:
            return 0.0
        return self.sum_of_samples / self.num_samples

    # NOTE: This is supposed to mimic this prometheus query:
    # rate(my_value_counter[{rate_secs}s])
    # This is useful for "average Counts per sec" metrics like throughput(QPS)
    def total_rate(self) -> float:
        """Calculates the total rate."""
        return self.sum_of_samples / self.num_rate_seconds

    @classmethod
    def merge(
        cls,
        rate_calculations: Iterable["RateCalculation"],
    ) -> "RateCalculation":
        """Calculates the average rate using multiple rate calculations."""
        total_sum_of_samples = 0.0
        total_num_samples = 0
        num_rate_seconds_for_group = None
        for rate_calculation in rate_calculations:
            total_sum_of_samples += rate_calculation.sum_of_samples
            total_num_samples += rate_calculation.num_samples
            if num_rate_seconds_for_group is None:
                num_rate_seconds_for_group = rate_calculation.num_rate_seconds
            elif num_rate_seconds_for_group != rate_calculation.num_rate_seconds:
                raise ValueError(
                    "Cannot average rate calculations with different num_rate_seconds"
                )

        if total_num_samples == 0 and num_rate_seconds_for_group is None:
            # This case should never happen, so raise an error to be safe
            raise ValueError(
                "Could not deteremine num_rate_seconds for group of rate calculations"
            )

        return cls(total_sum_of_samples, total_num_samples, num_rate_seconds_for_group)


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
        self._count_ray_counter = Counter(
            name=name, description=description, tag_keys=tag_keys
        )
        self._count_ray_counter.set_default_tags(default_tags)
        self._amount_ray_counter = Counter(
            name=name, description=description, tag_keys=tag_keys
        )
        self._amount_ray_counter.set_default_tags(default_tags)

    def inc(self, n: Union[int, float] = 1, *args, **kwargs):
        """Increments both the COUNT & AMOUNT counters and updates the rate buckets."""
        self._count_ray_counter.inc(1, *args, **kwargs)
        self._amount_ray_counter.inc(n, *args, **kwargs)
        self.update_rate_buckets(n)

    def empty_inc(self, *args, **kwargs):
        """Only increments the COUNT counter and updates the rate buckets."""
        self._count_ray_counter.inc(*args, **kwargs)
        self.update_rate_buckets(0)

    def update_rate_buckets(self, value: Union[int, float]):
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

    def calculate_rate(self) -> RateCalculation:
        return RateCalculation(self.running_sum, self.running_count, self.rate_secs)


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
        return self._latest_value
