<div align="center">

<img src="https://raw.githubusercontent.com/launchflow/buildflow/main/docs/images/buildflow-light.png#gh-light-mode-only" alt="BuildFlow Logo">
<img src="https://raw.githubusercontent.com/launchflow/buildflow/main/docs/images/buildflow-dark.png#gh-dark-mode-only" alt="BuildFlow Logo">

<hr>

### **⚒️ Build your entire system in minutes using pure Python. ⚒️**

![CI](https://github.com/launchflow/buildflow/actions/workflows/python_ci.yaml/badge.svg)
![Release Tests](https://github.com/launchflow/buildflow/actions/workflows/release_tests.yaml/badge.svg)
[![Python version](https://badge.fury.io/py/buildflow.svg)](https://pypi.org/project/buildflow)
[![codecov](https://codecov.io/gh/launchflow/buildflow/branch/main/graph/badge.svg?token=AO0TP8XG7X)](https://codecov.io/gh/launchflow/buildflow)
[![Slack Icon](https://img.shields.io/badge/slack-@launchflowusers-brightgreen.svg?logo=slack)](https://join.slack.com/t/launchflowusers/shared_invite/zt-27wlowsza-Uiu~8hlCGkvPINjmMiaaMQ)

</div>

## 📑 Resources

<div align="center">

📖 [Docs](https://docs.launchflow.com/buildflow/introduction) &nbsp; | &nbsp; ⚡ [Quickstart](https://docs.launchflow.com/buildflow/quickstart) &nbsp; | &nbsp; 👋 [Slack](https://join.slack.com/t/launchflowusers/shared_invite/zt-27wlowsza-Uiu~8hlCGkvPINjmMiaaMQ) &nbsp; | &nbsp; 🌟 [Contribute](https://docs.launchflow.com/buildflow/developers/contribute) &nbsp; | &nbsp; 🚀 [Deployment](https://www.launchflow.com/) &nbsp;

</div>

## 🤔 What is BuildFlow?

**BuildFlow** is a Python framework that allows you to build your entire backend system using one framework. With our simple decorator pattern you can turn any function into a component of your backend system. Allowing you to **serve data over HTTP, dump data to a datastore, or process async data from message queues**. All of these can use our built in IO connectors allowing you to create, manage, and connect to your cloud resources using pure Python.

### Key Features

#### Common Serving & Processing Patterns | 📖 [Docs](https://docs.launchflow.com/buildflow/concepts#processors)

Turn any function into a component of your backend system.

```python
# Serve traffic over HTTP or Websockets
service = app.service("my-service")
@service.endpoint("/", method="GET")
def get():
    return "Hello World"

# Collect, transform, and write data to storage
@app.collector("/collect", method="POST", sink=SnowflakeTable(...))
def collect(request: Dict[str, Any]):
  return element

# Process data from message queues such as Pub/Sub & SQS
@app.consumer(source=SQSQueue(...), sink=BigQuery(...))
def process(element: Dict[str, Any]):
    return element
```

#### Infrastructure from Code | 📖 [Docs](https://docs.launchflow.com/buildflow/guides/manage-cloud-resources)

Create and connect to cloud resources using python (powered by [Pulumi](https://github.com/pulumi/pulumi))

```python
# Use Python objects to define your infrastructure
sqs_queue = SQSQueue("queue-name")
gcs_bucket = GCSBucket("bucket-name")

# Your application manages its own infrastructure state
app.manage(s3_bucket, gcs_bucket)

# Use the same resource objects in your application logic
@app.consumer(source=sqs_queue, sink=gcs_bucket)
def process(event: YourSchema) -> OutputSchema:
    # Processing logic goes here
    return OutputSchema(...)
```

#### Dependency Injection | 📖 [Docs](https://docs.launchflow.com/buildflow/programming-guide/dependencies)

Inject any dependency with full control over its setup and lifecycle

```python
# Define custom dependencies
@dependency(Scope.GLOBAL)
class MyStringDependency:
    def __init__(self):
        self.my_string = "HELLO!"

# Or use the prebuilt dependencies
PostgresDep = SessionDepBuilder(postgres)

# BuildFlow handles the rest
@service.endpoint("/hello", method="GET")
def hello(db: PostgresDep, custom_dep: MyStringDependency):
    with db.session as session:
        user = session.query(User).first()
    # Returns "HELLO! User.name"
    return f"{custom_dep.my_string} {user.name}"
```

#### Async Runtime | 📖 [Docs](https://docs.launchflow.com/buildflow/programming-guide/processors#async-with-ray)

Scale out parallel tasks across your cluster with [Ray](https://docs.ray.io/en/latest/index.html) or any other async framework.

```python
@ray.remote
def long_task(elem):
    time.sleep(10)
    return elem

@app.consumer(PubSubSubscription(...), BigQueryTable(...))
def my_consumer(elem):
    # Tasks are automatically parallelized across your cluster
    return await long_task.remote(elem)
```

## ⚙️ Installation

```bash
pip install buildflow
```

### Extra Dependencies

#### Pulumi Installation

BuildFlow uses Pulumi to manage resources used by your application. To install Pulumi visit: https://www.pulumi.com/docs/install/

Installing Pulumi unlocks:

- allows BuildFlow to manage resource creation and destruction
- full access to Pulumi API / CLI
- fine-grained control over Pulumi Stacks & Resources

## 🩺 Code Health Checks

We use [black](https://github.com/psf/black) and [ruff](https://github.com/charliermarsh/ruff) with [pre-commit](https://pre-commit.com/) hooks to perform health checks.
To setup these locally:

- Clone the repo
- Install the `dev` dependencies like `python -m pip install -e .[dev]`
- Check if pre-commit is installed correctly by running `pre-commit --version`
- Setup pre-commit to run before every commit on staged changes by running `pre-commit install`
- Pre-commit can also be ran manually as `pre-commit run --all-files`

## 📜 License

BuildFlow is open-source and licensed under the [Apache License 2.0](LICENSE). We welcome contributions, see our [CONTRIBUTING.md](CONTRIBUTING.md) file for details.
