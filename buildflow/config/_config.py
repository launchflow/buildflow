class Config:
    @classmethod
    def default(cls) -> "Config":
        """Returns the default config."""
        raise NotImplementedError("default not implemented")

    @classmethod
    def load(cls, config_path: str) -> "Config":
        """Loads the config from the given path."""
        raise NotImplementedError("load not implemented")

    def dump(self, config_path: str):
        """Dumps the config to the given path."""
        raise NotImplementedError("dump not implemented")
