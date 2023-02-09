"""Base class for all Ray IO Connectors"""

import json
import logging
from typing import Any, Dict, Iterable, Union, Optional

from opentelemetry import propagate, trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor


PROPAGATOR = propagate.get_global_textmap()


trace.set_tracer_provider(
   TracerProvider(
       resource=Resource.create({SERVICE_NAME: "my-hello-service"})
   )
)

jaeger_exporter = JaegerExporter(
   agent_host_name="localhost",
   agent_port=6831,
)

trace.get_tracer_provider().add_span_processor(
   SimpleSpanProcessor(jaeger_exporter)
)

tracer = trace.get_tracer(__name__)

def _data_tracing_enabled() -> bool:
    # return 'ENABLE_FLOW_DATA_TRACING' in os.environ
    return True


def header_from_ctx(ctx, key):
    header = ctx.get(key)
    return [header] if header else []


def set_header_in_ctx(ctx, key, value):
    ctx[key] = value

def add_to_span(key: str, data: Union[Dict[str, Any], Iterable[Dict[str, Any]]], ctx: Dict[str, str]):
    with tracer.start_as_current_span(key, context=ctx) as span:
        span.set_attribute(key, json.dumps(data))


class RaySource:
    """Base class for all Ray sources."""

    def __init__(self, ray_inputs: Iterable, input_node_space: str) -> None:
        self.ray_inputs = ray_inputs
        self.input_node_space = input_node_space
        self.data_tracing_enabled = _data_tracing_enabled()

    def run(self):
        raise ValueError('All Ray sources should implement: `_`')


class RaySink:
    """Base class for all Ray sinks."""

    def __init__(self) -> None:
        self.data_tracing_enabled = _data_tracing_enabled()

    def _write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
    ):
        raise ValueError('All Ray sinks should implement: `_write`')

    def write(
        self,
        element: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
        ctx: Dict[str, str],
    ):
        if self.data_tracing_enabled:
            print('CREATING SPAN IN RAY SINK: ', ctx)
            add_to_span('output_data', element, ctx)
        return self._write(element)
