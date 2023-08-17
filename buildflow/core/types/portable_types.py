# TODO: Add comments to show the str patterns
# Optional TODO: Add post-init validation on the str format


# NOTE: These types should remain portable across all cloud providers. If a
# type is specific to a cloud provider, it should be defined in the types
# module for that cloud provider.


# These types let us set values on PortablePrimitives that are not
# cloud-provider specific. For example, we can set a TopicID on a
# Topic primitive, and then use that TopicID to create a GCP PubSub Topic.

# Table Types
TableName = str

TableID = str

# Topic Types
TopicID = str

TopicName = str

# Queue Types
QueueID = str

# Bucket Types
BucketName = str

# File Paths

# Subscription Types
SubscriptionName = str
