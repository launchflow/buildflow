import buildflow

from .ml_models import compute_gait
from .models import Gait, StepCount

node = buildflow.Node()


@node.processor(
    source=buildflow.PubSubSource(cloud="gcp", name="gait_pubsub"),
    sink=buildflow.DataWarhouse(cloud="gcp", name="gait"),
)
def gait(step_count: StepCount) -> Gait:
    return compute_gait(step_count)
