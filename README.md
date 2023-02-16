# Flow IO

![CI](https://github.com/launchflow/flow/actions/workflows/python_ci.yaml/badge.svg)

Flow IO is an open source framework to make it easier to for developers to
connect different resources to different frameworks. By using
Flow IO users can focus on writing their specific framework logic as opposed
to figuring out how exactly to connect to different resource types.

## IO Connector Support

Below are all resource / framework IO connections we support. If you would
like any additional frameworks or resources supported please file a
GitHub issue!

✅ = Fully Functional&nbsp;&nbsp;✏️ = Implementation in Progress&nbsp;&nbsp;❌ = Implementation on Backlog

|      | BigQuery | Google Pub/Sub | Redis Stream | DuckDB |
| ---- | -------- | -------------- | ------------ | ------ |
| Ray  | ✅       | ✅              | ✏️            | ✅     |
| Beam | ❌       | ❌              | ❌           | ❌      |


## LaunchFLow Integration

Flow IO is maintained by the team at LaunchFlow. LaunchFlow allows you to take
your usage of Flow IO and deploy all frameworks and resources to the cloud. If
you're interested in using LaunchFlow you can learn more at launchflow.com, or
reach out to us at founders@launchflow.com
