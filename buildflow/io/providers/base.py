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
