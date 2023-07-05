from typing import List

from buildflow.api.composites._composite import CompositeAPI


class PatternAPI:
    composites: List[CompositeAPI]

    def setup(self):
        raise NotImplementedError("setup not implemented")

    def process(self):
        raise NotImplementedError("process not implemented")
