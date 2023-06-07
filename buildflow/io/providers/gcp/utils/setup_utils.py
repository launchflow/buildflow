"""Utils for working with pubsub."""

import logging
from typing import List

from google.api_core import exceptions
from google.iam.v1 import iam_policy_pb2, policy_pb2

from buildflow.io.providers.gcp.utils import clients


def maybe_create_topic(
    *, pubsub_topic: str, billing_project: str, publisher_members: List[str] = []
):
    publisher_client = clients.get_publisher_client(billing_project)
    try:
        publisher_client.get_topic(topic=pubsub_topic)
    except exceptions.NotFound:
        logging.info(f"topic {pubsub_topic} not found attempting to create")
        try:
            logging.info(f"Creating topic: {pubsub_topic}")
            topic = publisher_client.create_topic(name=pubsub_topic)
            if publisher_members:
                iam_policy = publisher_client.get_iam_policy(
                    request=iam_policy_pb2.GetIamPolicyRequest(resource=topic.name)
                )
                added_publisher = False
                for binding in iam_policy.bindings:
                    if binding.role == "roles/pubsub.publisher":
                        binding.members.extend(publisher_members)
                        added_publisher = True
                if not added_publisher:
                    iam_policy.bindings.append(
                        policy_pb2.Binding(
                            role="roles/pubsub.publisher", members=publisher_members
                        )
                    )
                publisher_client.set_iam_policy(
                    request=iam_policy_pb2.SetIamPolicyRequest(
                        resource=topic.name, policy=iam_policy
                    )
                )
        except exceptions.PermissionDenied:
            raise ValueError(
                f"Failed to create topic: {pubsub_topic}. Please "
                "ensure you have permission to read the existing topic or "
                "permission to create a new topic if needed."
            )


def maybe_create_subscription(
    *,
    pubsub_subscription: str,
    pubsub_topic: str,
    billing_project: str,
    publisher_members: List[str] = [],
):
    subscriber_client = clients.get_subscriber_client(billing_project)
    try:
        subscriber_client.get_subscription(subscription=pubsub_subscription)
    except exceptions.NotFound:
        if not pubsub_topic:
            raise ValueError(
                f"subscription: {pubsub_subscription} was not found, "
                "please provide the topic so we can create the "
                "subscriber or ensure you have read access to the "
                "subscribe."
            )
        maybe_create_topic(
            pubsub_topic=pubsub_topic,
            publisher_members=publisher_members,
            billing_project=billing_project,
        )
        try:
            logging.info(f"Creating subscription: {pubsub_subscription}")
            subscriber_client.create_subscription(
                name=pubsub_subscription,
                topic=pubsub_topic,
                # TODO: we should make this
                # configurable.
                ack_deadline_seconds=600,
            )
        except exceptions.PermissionDenied:
            raise ValueError(
                f"Failed to create subscription: {pubsub_subscription}. "
                "Please ensure you have permission to read the "
                "existing subscription or permission to create a new "
                "subscription if needed."
            )
