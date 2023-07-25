import enum


# TODO: allow users to set as a string
class FileFormat(enum.Enum):
    # TODO: Support additional file formats (Arrow, Avro, etc..)
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"


class FileChangeStreamEvents(enum.Enum):
    CREATED = 1
    DELETED = 2
    MODIFIED = 3
    MOVED = 4
