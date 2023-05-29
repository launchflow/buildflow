import dataclasses


@dataclasses.dataclass
class RuntimeConfig:
    num_threads_per_process: int
    num_actors_per_core: int
    num_available_cores: int

    @classmethod
    def IO_BOUND(cls, num_available_cores: int):
        return cls(num_threads_per_process=20,
                   num_actors_per_core=1,
                   num_available_cores=num_available_cores)

    @classmethod
    def CPU_BOUND(cls, num_available_cores: int):
        return cls(num_threads_per_process=1,
                   num_actors_per_core=2,
                   num_available_cores=num_available_cores)

    @classmethod
    def DEBUG(cls):
        return cls(num_threads_per_process=1,
                   num_actors_per_core=1,
                   num_available_cores=1)

    def num_replicas(self):
        return self.num_actors_per_core * self.num_available_cores

    def num_cpus(self):
        return 1 / self.num_actors_per_core
