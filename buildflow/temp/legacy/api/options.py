from dataclasses import dataclass
from typing import Optional


@dataclass
class AutoscalingOptions:
    # The minimum number of replicas to maintain.
    min_replicas: int = 1
    # The maximum number of replicas to scale up to.
    max_replicas: int = 1000
    # The number of replicas to start your pipeline with. If not set we will
    # start with whatever min_replicas is set to.
    num_replicas: Optional[int] = None
    # Whether or not we should autoscale your number of replicas. If this is
    # set to false your pipeline will always maintain the same number of
    # replicas as it started with.
    autoscaling: bool = True


@dataclass
class ProcessorOptions:
    # The autoscaling options for the processor.
    autoscaling_options: AutoscalingOptions = AutoscalingOptions()
