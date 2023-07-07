import dataclasses
from typing import Optional

from buildflow.core import utils
from buildflow.core.io.resources._resource import Resource


@dataclasses.dataclass
class GCPPubSubTopic(Resource):
    project_id: str
    topic_name: Optional[str] = None
    exclude_from_infra: bool = False

    def __post_init__(self):
        if self.topic_name is None:
            project_hash = utils.stable_hash(self.project_id)
            self.topic_name = f"buildflow_topic_{project_hash[:8]}"


@dataclasses.dataclass
class GCPPubSubSubscription(Resource):
    project_id: str
    topic_id: str  # format: projects/{project_id}/topics/{topic_name}
    subscription_name: Optional[str] = None
    exclude_from_infra: bool = False

    def __post_init__(self):
        if self.subscription_name is None:
            topic_hash = utils.stable_hash(self.topic_id)
            self.subscription_name = f"buildflow_subscription_{topic_hash[:8]}"
