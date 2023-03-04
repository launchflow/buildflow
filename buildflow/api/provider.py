from buildflow.api.processor import ProcessorAPI


class ProviderAPI:

    # This instance method defines the reference to the managed Processor.
    def _processor(self) -> ProcessorAPI:
        raise NotImplementedError('_processor not implemented')
