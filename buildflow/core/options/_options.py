# NOTE: We use this to indicate that a value is not set. This is useful for
# distinguishing between a value that is set to None and a value that is not
# set at all.
NOT_SET = object()


class Options:
    @classmethod
    def default(cls) -> "Options":
        """Returns the default options."""
        raise NotImplementedError("default not implemented")
