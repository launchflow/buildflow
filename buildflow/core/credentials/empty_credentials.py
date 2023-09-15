from buildflow.core.credentials._credentials import Credentials


class EmptyCredentials(Credentials):
    """Empty credentials used by strategies that don't need credentials."""

    def __init__(self) -> None:
        super().__init__(None)
