from typing import List

from buildflow.api_v2.node.pattern.primitive import PrimitiveAPI


class PatternAPI:
    primitives: List[PrimitiveAPI]

    def setup(self):
        raise NotImplementedError("setup not implemented")
