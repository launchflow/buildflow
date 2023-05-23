from buildflow import Depends, PubSub
import buildflow

from .ml_models import classify_activity, count_steps
from .models import IMU, ActivityClassification, StepCount
from .gait import gait

node = buildflow.Node()


@node.processor(
    source=buildflow.PubSubSource(cloud="gcp", name="sc_pubsub"),
    sink=buildflow.DataWarhouse(cloud="gcp", name="step_count"),
)
def step_count(
    activity_classification: ActivityClassification,
    gait_pubsub: PubSub[StepCount] = Depends(gait),
) -> StepCount:
    sc = count_steps(activity_classification)
    gait_pubsub.publish(sc)
    return sc


@node.processor(
    source=buildflow.PubSubSource(cloud="gcp", name="ac_pubsub"),
    sink=buildflow.DataWarehouse(cloud="gcp", name="activity_classification"),
)
def activity_classification(
    imu: IMU, sc_pubsub: PubSub[ActivityClassification] = Depends(step_count)
) -> ActivityClassification:
    ac = classify_activity(imu)
    sc_pubsub.publish(ac)
    return ac
