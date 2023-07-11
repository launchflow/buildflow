import dataclasses
from datetime import datetime

from buildflow import Node, ResourcesConfig
from buildflow.resources import DataWarehouseTable, Topic


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


resource_config = ResourcesConfig.default()
# resource_config.gcp.default_project_id = "daring-runway-374503"
# Create a new Node with custom resource config
app = Node(resource_config=resource_config)


# Define the source and sink
source = Topic(
    resource_id="tanke_topic_id",
    resource_provider="gcp",
)
sink = DataWarehouseTable(
    resource_id="tanke_table_id",
    resource_provider="gcp",
)


# Attach a processor to the Node
@app.processor(source=source, sink=sink)
def my_processor(pubsub_message: TaxiOutput) -> TaxiOutput:
    return pubsub_message


# print(dataclasses.asdict(my_processor.source().resource_type()))
# print(dataclasses.asdict(my_processor.sink().resource_type()))

# if __name__ == "__main__":
#     app.run(
#         disable_usage_stats=True,
#         # runtime-only options
#         block_runtime=True,
#         debug_run=False,
#         # infra-only options.
#         apply_infrastructure=False,
#         # Ad hoc infra is really nice for quick demos / tests
#         destroy_infrastructure=False,
#         # server options
#         start_node_server=True,
#     )

# these should also work:
# app.plan()
# app.apply()
# app.destroy()
