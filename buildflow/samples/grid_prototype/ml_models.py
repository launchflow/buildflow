from .entities import IMU, ActivityClassification, StepCount, Gait

def classify_activity(imu: IMU) -> ActivityClassification:
    return ActivityClassification(activity="walking")

def count_steps(activity_classification: ActivityClassification) -> StepCount:
    return StepCount(steps=100)

def compute_gait(step_count: StepCount) -> Gait:
    return Gait(gait="normal")