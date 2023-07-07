from buildflow.core.io import AnalysisTable
from buildflow.core.app.flow import Flow
from buildflow.core.options import (
    FlowOptions,
    AutoscalerOptions,
    RuntimeOptions,
    InfraOptions,
    PrimitiveOptions,
)
from buildflow.core.io.primitives.gcp import GCPPubSubSubscription, BigQueryTable
from sandbox4 import TaxiOutput

# Create a new Flow
app = Flow(
    flow_options=FlowOptions(
        infra_options=InfraOptions.default(),
        primitive_options=PrimitiveOptions.default(),
        runtime_options=RuntimeOptions(
            processor_options={},
            autoscaler_options=AutoscalerOptions(
                enable_autoscaler=False,
                min_replicas=1,
                max_replicas=10,
                log_level="INFO",
            ),
            num_replicas=10,
            log_level="INFO",
        ),
    )
)


# Define the source and sink
pubsub_source = GCPPubSubSubscription(
    topic_id="projects/pubsub-public-data/topics/taxirides-realtime",
    project_id="daring-runway-374503",
    subscription_name="taxiride-sub",
)
bigquery_sink = BigQueryTable(
    project_id="daring-runway-374503",
    dataset_name="buildflow",
    table_name="table_9312c458",
)


# Attach a processor to the Flow
@app.pipeline(
    source=pubsub_source,
    sink=bigquery_sink,
    num_cpus=1.0,
    num_concurrency=8,
)
def my_processor(pubsub_message: TaxiOutput) -> TaxiOutput:
    return pubsub_message


app.run(disable_usage_stats=True, start_runtime_server=True)
