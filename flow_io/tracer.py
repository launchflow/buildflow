# flake8: noqa
from typing import Any, Dict, Iterable, Union
import json
import redis
import uuid
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


class Tracer(object):

    def add_to_trace(self,
                     key: str,
                     data: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
                     carrier: Dict[str, str] = {}):
        raise NotImplementedError(
            f'`add_to_trace` has not been implemented for {self.__class__.__name__}'
        )


class OpenTelemetryTracer(Tracer):

    def __init__(self):
        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource.create({SERVICE_NAME: "my-service"})))
        jaeger_exporter = JaegerExporter(
            agent_host_name="jaeger",
            agent_port=6831,
        )
        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(jaeger_exporter))
        self._tracer = trace.get_tracer(__name__)

    def add_to_trace(self,
                     key: str,
                     data: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
                     carrier: Dict[str, str] = {}):
        ctx = TraceContextTextMapPropagator().extract(carrier=carrier)
        with self._tracer.start_as_current_span(key, context=ctx) as span:
            carrier = {}
            TraceContextTextMapPropagator().inject(carrier)
            span.set_attribute(key, json.dumps(data))
            return carrier


class RedisTracer(Tracer):

    def __init__(self):
        self._r = redis.Redis(host='redis', port=6381, db=0)

    def add_to_trace(self,
                     key: str,
                     data: Union[Dict[str, Any], Iterable[Dict[str, Any]]],
                     carrier: Dict[str, str] = {}):
        trace_id = carrier.get('trace_id', uuid.uuid4().hex)
        self._r.xadd(trace_id, {key: json.dumps(data)})
        # merges the dicts together to keep other keys in the downstream carrier
        return {**{'trace_id': trace_id}, **carrier}
