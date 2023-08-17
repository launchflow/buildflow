import dataclasses

from buildflow.core.types.portable_types import BucketName
from buildflow.io.primitive import PortablePrimtive


@dataclasses.dataclass
class Bucket(PortablePrimtive):
    bucket_name: BucketName
