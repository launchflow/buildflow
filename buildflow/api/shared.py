import enum


class DefaultType(enum.Enum):
    IO_BOUND = "io_bound"
    CPU_BOUND = "cpu_bound"
    DEBUG = "debug"


# NOTE: Options are similar to Configs, but they are not responsible for persisting
# to disk. A Config may contain Options and may persist them to disk, but an Option
# is not responsible for persisting itself to disk.
class Options:
    @classmethod
    def default(cls, default_type: DefaultType) -> "Options":
        """Returns the default options."""
        raise NotImplementedError("default not implemented")


class Config:
    @classmethod
    def default(cls, default_type: DefaultType) -> "Config":
        """Returns the default config."""
        raise NotImplementedError("default not implemented")

    @classmethod
    def load(cls, config_path: str) -> "Config":
        """Loads the config from the given path."""
        raise NotImplementedError("load not implemented")

    def dump(self, config_path: str):
        """Dumps the config to the given path."""
        raise NotImplementedError("dump not implemented")


class State:
    @classmethod
    def initial(cls) -> "State":
        """Returns the initial state."""
        raise NotImplementedError("initial not implemented")

    @classmethod
    def load(cls, state_path: str) -> "State":
        """Loads the state from the given path."""
        raise NotImplementedError("load not implemented")

    def dump(self, state_path: str):
        """Dumps the state to the given path."""
        raise NotImplementedError("dump not implemented")

    def as_dict(self):
        """Returns the state as a dictionary."""
        raise NotImplementedError("as_dict not implemented")
