# jaeger_tracing.py
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
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
   BatchSpanProcessor(jaeger_exporter)
)
tracer = trace.get_tracer(__name__)
with tracer.start_as_current_span("rootSpan"):
   with tracer.start_as_current_span("childSpan"):
           print("Hello world!")



resource/queue/pubsub/regular/node_e8fe

service/processor/ray/core/node_29b8

resource/storage/bigquery/table/node_c449




"""This file is used to run your ray core DAG."""

from opentelemetry import trace
import ray

import flow_io
import logging
import inspect

@ray.remote
class ProcessorActor:

    def __init__(self, sink):
        self.sink = sink

    def process(self, ray_input, carrier):
        print("PROCESSING: ", carrier)
        output = self._process(ray_input)
        # Here we call `sink.write` to send the output to
        # all outputs configured in our flow.
        return ray.get(self.sink.write.remote(output, carrier))

    def _process(self, ray_input):
        # TODO: add logic here.
        return ray_input


# This sink can be used to write data to outputs configured
# in your flow.
sink = flow_io.ray_io.sink()

# Here we pass in the sink to the actor allowing the actor to
# write data to all outputs.
processor = ProcessorActor.remote(sink)

source = flow_io.ray_io.source(processor.process)

# source.run.remote() will start our workflow. It will read data
# from your input node configured in your flow.
ray.get(source.run.remote())
