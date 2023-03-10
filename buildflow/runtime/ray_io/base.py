"""Base class for all Ray IO Connectors"""

import asyncio
import dataclasses
import os
from typing import Any, Callable, Dict, Iterable, Union

import ray
from buildflow.runtime import tracer as t
from buildflow import utils

tracer = t.RedisTracer()


def add_to_trace(
    key: str,
    value: Any,
    context: Dict[str, str],
) -> Dict[str, str]:
    """Add a key value pair to the current span.

    Args:
        key: The key to add to the span.
        value: The value to add to the span.
        context: The context state handler.

    Returns:
        The updated context.
    """
    return tracer.add_to_trace(key, value, context)


def _data_tracing_enabled() -> bool:
    return 'ENABLE_FLOW_DATA_TRACING' in os.environ


class RaySink:
    """Base class for all ray sinks."""

    def __init__(self, remote_fn: Callable) -> None:
        self.remote_fn = remote_fn
        self.data_tracing_enabled = _data_tracing_enabled()

    async def _write(
        self,
        elements: Union[ray.data.Dataset, Iterable[Any]],
    ):
        raise NotImplementedError(
            f'`_write` method not implemented for class {self.__class__}')

    async def write(
        self,
        elements: Union[ray.data.Dataset, Iterable[Dict[str, Any]]],
        context: Dict[str, str] = {},
    ):
        if isinstance(elements, ray.data.Dataset):
            # If the elements are a ray dataset, we need to wrap in a list
            # since the remote function expects a list of elements.
            results = await self.remote_fn([elements])[0]
        else:
            temp_results = await self.remote_fn(elements)
            results = []
            for result in temp_results:
                if isinstance(result, (tuple, list)):
                    middle_result = []
                    for elem in result:
                        if dataclasses.is_dataclass(elem):
                            middle_result.append(utils.asdict(elem))
                        else:
                            middle_result.append(elem)
                    results.append(middle_result)
                elif dataclasses.is_dataclass(result):
                    results.append(utils.asdict(result))
                else:
                    results.append(result)

        if self.data_tracing_enabled:
            add_to_trace(key=self.__class__.__name__,
                         value={'output_data': results},
                         context=context)

        return await self._write(results)


class RaySource:
    """Base class for all ray sources."""

    def __init__(self, ray_sinks: Dict[str, RaySink]) -> None:
        self.ray_sinks = ray_sinks
        self.data_tracing_enabled = _data_tracing_enabled()

    async def run(self):
        raise NotImplementedError(
            f'`run` method not implemented for class {self.__class__}')

    @staticmethod
    def recommended_num_threads():
        # Each class that implements RaySource can use this to suggest a good
        # number of threads to use when creating the source in the runtime.
        return 1

    @classmethod
    def source_args(cls, io_ref, num_replicas: int):
        """Creates the inputs for the source replicas.

        This will be executed only once per runtime before the flow is actually
        started. It is a good place to do work that can not be serialized
        across processes. The best example of this is executing a bigquery
        query, then the subsequent actors can each read a portion of that
        query.
        """
        return [(io_ref, )] * num_replicas

    async def _send_batch_to_sinks_and_await(self, elements):
        result_keys = []
        task_refs = []
        for name, ray_sink in self.ray_sinks.items():
            # Processes the elements as a single batch
            task_ref = ray_sink.write.remote(elements)
            result_keys.append(name)
            task_refs.append(task_ref)
        result_values = await asyncio.gather(*task_refs)
        return dict(zip(result_keys, result_values))

    def shutdown(self):
        """Performs any shutdown work that is needed for the actor.

        Returns true if the program should block on any tasks create by this
        actor before allowing the program to finish. This is used by pub/sub
        to ensure we have acked all of our pending messages before exiting.
        """
        return False
