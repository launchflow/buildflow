from buildflow.api.processor import ProcessorAPI


class ProviderAPI:

    # This static method defines the reference the managed Processor.
    @staticmethod
    def _processor() -> ProcessorAPI:
        raise NotImplementedError('_processor not implemented')
