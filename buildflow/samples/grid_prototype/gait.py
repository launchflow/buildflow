import buildflow

from .ml_models import compute_gait
from .entities import Gait, StepCount

node = buildflow.Node()


@node.processor(
    source=buildflow.PubSubSource(cloud="gcp", name="gait_pubsub", project_id='daring-runway-374503'),
    sink=buildflow.DataWarehouseSink(cloud="gcp", name="gait", project_id='daring-runway-374503'),
)
def gait(step_count: StepCount) -> Gait:
    return compute_gait(step_count)
