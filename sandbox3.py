from buildflow.core.app.flow import Flow
from buildflow.core.runner._runner import Runner
from buildflow.core.io.resources.gcp.pubsub import GCPPubSubSubscription
from buildflow.api.primitives.db.table import AnalysisTable
from sandbox4 import TaxiOutput


# Create a new Flow
app = Flow()


# Define the source and sink
pubsub_source = GCPPubSubSubscription(
    topic_id="projects/pubsub-public-data/topics/taxirides-realtime",
    project_id="daring-runway-374503",
    subscription_name="taxiride-sub",
    exclude_from_infra=True,
)
pubsub_sink = AnalysisTable()


# Attach a processor to the Flow
@app.processor(
    source=pubsub_source,
    sink=pubsub_sink,
    num_cpus=0.5,
    num_concurrency=8,
)
def my_processor(pubsub_message: TaxiOutput) -> TaxiOutput:
    return pubsub_message


# Runner().apply(app)
Runner().run(app, disable_usage_stats=True, start_server=True)
