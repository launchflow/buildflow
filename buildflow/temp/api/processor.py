ProcessorID = str


class ProcessorAPI:
    processor_id: ProcessorID

    def setup(self):
        raise NotImplementedError("setup not implemented")

    def process(self, *args, **kwargs):
        raise NotImplementedError("process not implemented for Processor")
