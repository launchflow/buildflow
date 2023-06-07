from dataclasses import dataclass

from buildflow import Node
from buildflow.io import GCPPubSubSubscription, BigQueryTable


PROJECT_ID = ""
TOPIC_ID = ""
TABLE_ID = ""


def process_message(message):
    return message


# Use dataclasses to unlock validation
@dataclass
class InputSchema:
    field: str
    other: int


@dataclass
class OutputSchema:
    field: str
    other: int


"""
All systems built with BuildFlow will automatically:
  -> Create any cloud infrastructure they depends on (powered by pulumi)
  -> Autoscale the workload across all available CPU cores (powered by ray)
  -> Validate Schemas using Python's type system and dataclasses
  -> Export runtime metrics (per replica), such as latency, throughput, etc
"""


# Step 1. Create a BuildFlow Node (app)
app = Node()


# Step 2. Create a Processor that reads from Google PubSub and writes to BigQuery
@app.processor(
    source=GCPPubSubSubscription(topic_id=TOPIC_ID),
    sink=BigQueryTable(table_id=TABLE_ID),
)
def stream_processor(pubsub_message: InputSchema) -> OutputSchema:
    # The return value is automatically sent to the Sink (output)
    return process_message(pubsub_message)


# Step 3. Start the BuildFlow Runtime
app.run()
