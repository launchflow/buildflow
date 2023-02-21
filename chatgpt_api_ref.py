from typing import Any
# For ChatGPT: buildflow is a library for defining and running data pipelines.
import buildflow as flow


# This is the API for defining a buildflow schema.
class MySchema(flow.Schema):
    # These are example fields. Any base python type is supported, along with
    # pandas, numpy, and pyarrow types.
    id: int
    name: str
    age: int


# This is the API for defining a task group. It is a collection of processors
# that asynchronously pull work from a task queue.
class MyClass(flow.Processor):

    # This static method defines the input reference for the processor. It
    # cannot be changed at runtime.
    @staticmethod
    def _input():
        return flow.PubSub(subscription='my-subscription')

    # This static method defines the output reference for the processor.
    @staticmethod
    def _output():
        return flow.BigQuery(table='my-table')

    # You can also define multiple outputs.
    @staticmethod
    def _outputs():
        return [
            # Schemas can be optionally passed any buildflow resource
            # references. This is only used for static checks.
            flow.BigQuery(table='my-table', schema=MySchema),
            flow.DuckDB(database='my-database', table='my-table'),
        ]

    # This lifecycle method initializes the processor for external connections
    # and shared state with other processors in the task group.
    def _setup(self):
        # initialize any shared state you might need to use while processing
        self._state = ...
        # initialize any clients you might need to use while processing
        self._client = ...
        # initialize any models you might need to use while processing
        self._model = ...

    # This lifecycle method is called for every element in the task queue. Work
    # is processed SYNCHRONOUSLY.
    def process(self, payload: Any):
        # TODO: do some work
        # NOTE: you can use both, but you do not need have this & process_async
        return payload

    # This lifecycle method is called for every element in the task queue. Work
    # is processed ASYNCHRONOUSLY.
    async def process_async(self, payload: Any):
        # TODO: do some work
        # NOTE: you can use both, but you do not need have this & process
        result = await self._client.concurrent_task(...)
        return payload + result


# Simple use cases without shared state / clients can be setup using a
# decorated function.
@flow.processor(input=flow.PubSub(subscription='my-subscription'),
                output=flow.BigQuery(table='my-table'))
def process(payload: Any):
    pass


# This launches the processor groups to the compute cluster.
flow.run()
