import io
import json
import os
import sys
import unittest
from unittest import mock

from buildflow.core.app.infra.actors import infra
from buildflow.core.options.infra_options import InfraOptions
from buildflow.io.gcp import (
    CloudSQLDatabase,
    CloudSQLInstance,
    GCPPubSubSubscription,
    GCPPubSubTopic,
)
from buildflow.types.gcp import CloudSQLDatabaseVersion, CloudSQLInstanceSettings

_PREVIEW_OUTPUT = """\
Primitives to create:
---------------------
├── GCPPubSubTopic
│   └── testing-gcp-project/checkin-topic
│   └── testing-gcp-project/deployment-action-topic
│   └── testing-gcp-project/gcp-infra-creation-topic2
├── GCPPubSubSubscription
│   └── testing-gcp-project/deployment-action-subscription2
│   └── testing-gcp-project/gcp-infra-creation-subscription2
└── CloudSQLDatabase
    └── testing-gcp-project:launchflow-cloud-sql.launchflow-db


Primitives to destroy:
----------------------
├── GCPPubSubSubscription
│   └── caleb-launchflow-sandbox/deployment-action-subscription
│   └── caleb-launchflow-sandbox/gcp-infra-creation-subscription
└── GCPPubSubTopic
    └── caleb-launchflow-sandbox/gcp-infra-creation-topic


Primitives to update:
---------------------
└── GCPPubSubSubscription
    └── testing-gcp-project/checkin-subscription
        └── ackDeadlineSeconds
        └── messageRetentionDuration"""


class InfraTest(unittest.TestCase):
    def setUp(self) -> None:
        self.held_output = io.StringIO()
        sys.stdout = self.held_output

    def tearDown(self):
        self.held_output.close()
        sys.stdout = sys.__stdout__

    @mock.patch("buildflow.core.app.infra.actors.infra.PulumiWorkspace")
    async def test_infra_preview(self, workspace_mock: mock.MagicMock):
        checkin_topic = GCPPubSubTopic(
            project_id="testing-gcp-project", topic_name="checkin-topic"
        )
        checkin_subscription = GCPPubSubSubscription(
            project_id="testing-gcp-project", subscription_name="checkin-subscription"
        ).options(
            topic=checkin_topic, ack_deadline_seconds=2, message_retention_duration="1m"
        )

        deployment_action_topic = GCPPubSubTopic(
            project_id="testing-gcp-project", topic_name="deployment-action-topic"
        )
        deployment_action_subscription = GCPPubSubSubscription(
            project_id="testing-gcp-project",
            subscription_name="deployment-action-subscription2",
        ).options(topic=deployment_action_topic)

        gcp_infra_creation_topic = GCPPubSubTopic(
            project_id="testing-gcp-project", topic_name="gcp-infra-creation-topic2"
        )
        gcp_infra_creation_subscription = GCPPubSubSubscription(
            project_id="testing-gcp-project",
            subscription_name="gcp-infra-creation-subscription2",
        ).options(topic=gcp_infra_creation_topic)
        cloud_sql_instance = CloudSQLInstance(
            instance_name="launchflow-cloud-sql",
            project_id="testing-gcp-project",
            database_version=CloudSQLDatabaseVersion.POSTGRES_15,
            region="us-central1",
            settings=CloudSQLInstanceSettings(
                tier="database-tier",
            ),
        )
        cloud_sql_database = CloudSQLDatabase(
            instance=cloud_sql_instance,
            database_name="launchflow-db",
        )

        managed_primitives = {
            checkin_topic.primitive_id(): checkin_topic,
            checkin_subscription.primitive_id(): checkin_subscription,
            deployment_action_topic.primitive_id(): deployment_action_topic,
            deployment_action_subscription.primitive_id(): deployment_action_subscription,  # noqa
            gcp_infra_creation_topic.primitive_id(): gcp_infra_creation_topic,
            gcp_infra_creation_subscription.primitive_id(): gcp_infra_creation_subscription,  # noqa
            cloud_sql_instance.primitive_id(): cloud_sql_instance,
            cloud_sql_database.primitive_id(): cloud_sql_database,
        }

        plan_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "testing",
            "pulumi_plan.json",
        )
        with open(plan_path, "r") as f:
            plan_result = json.load(f)

        async def preview_fn(*args, **kwargs):
            return mock.MagicMock(plan_result=plan_result)

        workspace_mock.return_value.preview.side_effect = preview_fn

        stack_state = mock.MagicMock()
        stack_state.outputs = {}

        def resources():
            parent = "urn:pulumi:local::launchflow-cloud::buildflow:GCPPubSubSubscription::caleb-launchflow-sandbox/checkin-subscription"  # noqa
            base_resource = mock.MagicMock()
            base_resource.parent = parent
            return {
                "urn:pulumi:local::launchflow-cloud::buildflow:GCPPubSubSubscription$gcp:pubsub/subscription:Subscription::caleb-launchflow-sandbox-checkin-subscription": base_resource,  # noqa
                parent: mock.MagicMock(
                    resource_outputs={
                        "primitive_id": "testing-gcp-project/checkin-subscription"
                    }
                ),
                "to-delete1": mock.MagicMock(
                    resource_outputs={
                        "primitive_id": "caleb-launchflow-sandbox/deployment-action-subscription",  # noqa
                        "primitive_type": "GCPPubSubSubscription",
                    }
                ),
                "to-delete2": mock.MagicMock(
                    resource_outputs={
                        "primitive_id": "caleb-launchflow-sandbox/gcp-infra-creation-subscription",  # noqa
                        "primitive_type": "GCPPubSubSubscription",
                    }
                ),
                "to-delete3": mock.MagicMock(
                    resource_outputs={
                        "primitive_id": "caleb-launchflow-sandbox/gcp-infra-creation-topic",  # noqa
                        "primitive_type": "GCPPubSubTopic",
                    }
                ),
                "existing": mock.MagicMock(
                    resource_outputs={
                        "primitive_id": cloud_sql_instance.primitive_id(),
                        "primitive_type": type(cloud_sql_instance).__name__,
                    }
                ),
            }

        stack_state.resources = resources

        workspace_mock.return_value.get_stack_state.return_value = stack_state

        infra_actor = infra.InfraActor(InfraOptions.default(), None)
        await infra_actor.preview(
            pulumi_program=lambda x: x, managed_primitives=managed_primitives
        )

        self.held_output.seek(0)
        captured_output = self.held_output.read().strip()

        self.assertEqual(captured_output, _PREVIEW_OUTPUT)


if __name__ == "__main__":
    unittest.main()
