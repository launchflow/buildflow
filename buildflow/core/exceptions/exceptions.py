class CannotConvertSourceException(Exception):
    """Raised when a source cannot be converted to the desired type."""

    def __init__(self, message) -> None:
        long_message = (
            "Failed to convert for your source. Please ensure your input types"
            " are convertable from your sources preffered type. "
            f"For more info see: TODO add docs.\n\nFull error: {message}"
        )
        super().__init__(long_message)


class CannotConvertSinkException(Exception):
    """Raised when a source cannot be converted to the desired type."""

    def __init__(self, message) -> None:
        long_message = (
            "Failed to convert for your sink. Please ensure your input types"
            " are convertable from your sinks preffered type. "
            f"For more info see: TODO add docs.\n\nFull error: {message}"
        )
        super().__init__(long_message)


class PathDoesNotExistException(Exception):
    """Raised when a path does not exist."""

    def __init__(self, message) -> None:
        long_message = (
            "Failed to find a valid path. Please ensure your path exists. "
            f"For more info see: TODO add docs.\n\nFull error: {message}"
        )
        super().__init__(long_message)
