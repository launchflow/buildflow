import dataclasses


@dataclasses.dataclass
class RuntimeConfig:
    # initial setup options
    num_threads_per_process: int
    num_actors_per_core: int
    num_available_cores: int
    # autoscale options
    autoscale: bool = True
    min_replicas: int = 1
    max_replicas: int = 1000
    # misc
    log_level: str = "INFO"

    @classmethod
    def IO_BOUND(cls, num_available_cores: int):
        return cls(
            num_threads_per_process=10,
            num_actors_per_core=1,
            num_available_cores=num_available_cores,
        )

    @classmethod
    def CPU_BOUND(cls, num_available_cores: int):
        return cls(
            num_threads_per_process=1,
            num_actors_per_core=2,
            num_available_cores=num_available_cores,
        )

    @classmethod
    def DEBUG(cls):
        return cls(
            num_threads_per_process=1,
            num_actors_per_core=1,
            num_available_cores=1,
            autoscale=False,
            log_level="DEBUG",
        )

    def num_replicas(self):
        return self.num_actors_per_core * self.num_available_cores

    def num_worker_cpus(self):
        return 1 / self.num_actors_per_core
