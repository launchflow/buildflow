from buildflow.api.composites.sink import Sink
from buildflow.api.composites.source import Source
from buildflow.api.patterns.processor import Processor
from buildflow.core.io.providers._provider import Provider

# ProcessorProvider is a Processor + Provider type.
# It can be used as a Processor or a Provider.
ProcessorProvider = type("ProcessorProvider", (Processor, Provider), {})

# SourceProvider is a Source + Provider type.
# It can be used as a Source or a Provider.
SourceProvider = type("SourceProvider", (Source, Provider), {})


# SinkProvider is a Sink + Provider type.
# It can be used as a Sink or a Provider.
SinkProvider = type("SinkProvider", (Sink, Provider), {})
