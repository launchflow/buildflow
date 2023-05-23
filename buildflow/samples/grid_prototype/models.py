from dataclasses import dataclass

@dataclass
class IMU:
    x: float
    y: float
    z: float

@dataclass
class ActivityClassification:
    activity: str

@dataclass
class StepCount:
    steps: int

@dataclass
class Gait:
    gait: str