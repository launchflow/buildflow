from dataclasses import dataclass
from datetime import datetime
from sqlalchemy.orm import Session

from buildflow import Node, Setup, InfraOptions

# buildflow.resources exposes all of the declarative types (for Provider & Depends API).
# they use the io & client submodules to provide the actual implementations.
from buildflow.resources import RelationalDB, DataWarehouse, Queue


# Setup(Resource) will return the client/resource instance, not io/resource.
# Setup(fn) will return the result of calling the function.

Resource.setup()
Resource.client()

# using this option REQUIRES that all schemas are provided as type annotations.


@dataclass
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


# declarative types could have "local" implementations for testing.
# this is akin to our in-memory metrics vs prometheus.
app = Node(infra_config=InfraOptions(resource_provider="local"))


# using declarative api REQUIRES all schema types to be provided.
# Users can optionally pass an id for other resources to reference.
# We could also pass along any args, kwargs to the underlying resource(s).
@app.processor(source=Queue(id=...), sink=DataWarehouse(id=...))
def my_processor(
    # Depends(Resource) will also return the client/resource instance, not io/resource.
    pubsub_message: TaxiOutput,
    db: Session = Setup(RelationalDB(id=...)),
) -> TaxiOutput:
    return pubsub_message


if __name__ == "__main__":
    app.run(
        disable_usage_stats=True,
        apply_infrastructure=False,
        destroy_infrastructure=False,
        start_node_server=True,
    )
