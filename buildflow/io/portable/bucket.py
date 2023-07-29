import dataclasses

from buildflow.core.io.primitive import PortablePrimtive
from buildflow.core.types.portable_types import BucketName


@dataclasses.dataclass
class Bucket(PortablePrimtive):
    bucket_name: BucketName
