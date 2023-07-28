import dataclasses
import os
from datetime import datetime
from typing import Any, Dict

import buildflow
from buildflow.core.io.portable import AnalysisTable, Topic

bigquery_table = os.getenv("BIGQUERY_TABLE", "taxi_rides")

# Set up a subscriber for the source.
# If this subscriber does not exist yet BuildFlow will create it.
input_source = Topic(topic_id="projects/pubsub-public-data/topics/taxirides-realtime")
# Set up a BigQuery table for the sink.
# If this table does not exist yet BuildFlow will create it.
output_table = AnalysisTable(bigquery_table).options(destroy_protection=False)


# Define an output type for our pipeline.
# By using a dataclass we can ensure our python type hints are validated
# against the BigQuery table's schema.
@dataclasses.dataclass
class TaxiOutput:
    ride_id: str
    point_idx: int
    latitude: float
    longitude: float
    timestamp: datetime
    meter_reading: float
    meter_increment: float
    ride_status: str
    passenger_count: int


app = buildflow.Flow(flow_options=buildflow.FlowOptions(require_confirmation=False))


# Define our processor.
@app.pipeline(source=input_source, sink=output_table)
def process(element: Dict[str, Any]) -> TaxiOutput:
    return TaxiOutput(**element)
