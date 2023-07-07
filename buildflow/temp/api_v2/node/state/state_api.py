class StateAPI:
    @classmethod
    def initial(cls) -> "StateAPI":
        """Returns the initial state."""
        raise NotImplementedError("initial not implemented")

    @classmethod
    def load(cls, state_path: str) -> "StateAPI":
        """Loads the state from the given path."""
        raise NotImplementedError("load not implemented")

    def dump(self, state_path: str):
        """Dumps the state to the given path."""
        raise NotImplementedError("dump not implemented")
