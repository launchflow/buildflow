from buildflow import Depends, PubSub
import buildflow

from .ml_models import classify_activity, count_steps
from .entities import IMU, ActivityClassification, StepCount
from .gait import gait

node = buildflow.Node()


@node.processor(
    source=buildflow.PubSubSource(cloud="gcp", name="sc_pubsub", project_id='daring-runway-374503'),
    sink=buildflow.DataWarehouseSink(cloud="gcp", name="step_count", project_id='daring-runway-374503'),
)
def step_count(
    activity_classification: ActivityClassification,
    gait_pubsub: PubSub[StepCount] = Depends(gait),
) -> StepCount:
    sc = count_steps(activity_classification)
    gait_pubsub.publish(sc)
    return sc


@node.processor(
    source=buildflow.PubSubSource(cloud="gcp", name="ac_pubsub", project_id='daring-runway-374503'),
    sink=buildflow.DataWarehouseSink(cloud="gcp", name="activity_classification", project_id='daring-runway-374503'),
)
def activity_classification(
    imu: IMU, sc_pubsub: PubSub[ActivityClassification] = Depends(step_count)
) -> ActivityClassification:
    ac = classify_activity(imu)
    sc_pubsub.publish(ac)
    return ac



if __name__ == "__main__":
    node.run()
