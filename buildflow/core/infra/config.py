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
    require_confirmation: bool = True
    # misc
    log_level: str = "INFO"

    @classmethod
    def DEBUG(cls):
        return cls(
            schema_validation=SchemaValidation.LOG_WARNING,
            require_confirmation=True,
            log_level="INFO",
        )
