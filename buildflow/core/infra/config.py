import dataclasses
import enum


class SchemaValidation(enum.Enum):
    STRICT = enum.auto()
    LOG_WARNING = enum.auto()
    NONE = enum.auto()


@dataclasses.dataclass
class InfraConfig:
    # plan options
    schema_validation: SchemaValidation = SchemaValidation.STRICT
    # apply & destroy options (These are basically inverses of eachother)
    # TODO: make this default to True once users can provide input
    require_confirmation: bool = False
    # misc
    log_level: str = "INFO"

    @classmethod
    def DEBUG(cls):
        return cls(
            schema_validation=SchemaValidation.LOG_WARNING,
            require_confirmation=False,
            log_level="DEBUG",
        )

    @classmethod
    def DEFAULT(cls):
        return cls(
            schema_validation=SchemaValidation.LOG_WARNING,
            require_confirmation=False,
            log_level="INFO",
        )
