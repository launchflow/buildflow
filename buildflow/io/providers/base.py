class Batch:

    @classmethod
    def empty(cls):
        raise NotImplementedError('empty not implemented')

    def __iter__(self):
        raise NotImplementedError('__iter__ not implemented')


class ProviderAPI:

    def __init__(self):
        pass

    def schema(self):
        raise NotImplementedError('schema not implemented')


class PullProvider(ProviderAPI):

    async def pull(self) -> Batch:
        raise NotImplementedError('pull not implemented')

    async def ack(self):
        raise NotImplementedError('ack not implemented')

    async def backlog(self) -> int:
        raise NotImplementedError('backlog not implemented')


class PushProvider(ProviderAPI):

    async def push(self, batch: Batch):
        raise NotImplementedError('push not implemented')


# TODO: Should we have InfraProvider instead that had plan, apply, destroy?
# Thoughts: Its nice to have a setup() option as a scape goat option for users
# who dont want to implement the Infra api for their custom use case.
class SetupProvider(ProviderAPI):

    # TODO: We should probably return something like SetupResult that can
    # include error messages for failed setup runs.
    async def setup(self) -> bool:
        raise NotImplementedError('setup not implemented')
