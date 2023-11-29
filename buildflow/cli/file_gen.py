# ruff: noqa
def hello_world_settings_template() -> str:
    return """\
\"\"\"This file defines any environment variables needed to run.\"\"\"

import os
from enum import Enum

import dotenv


class Environment(Enum):
    DEV = "dev"
    PROD = "prod"
    LOCAL = "local"


class Settings:
    def __init__(self) -> None:
        dotenv.load_dotenv()
        self.env = Environment(os.getenv("ENV", "local"))


env = Settings()
"""


def hello_world_primitives_template() -> str:
    return """\
\"\"\"This file contains any primitive definitions that you may need in your flow.

For example if you needed a remote storagebucket:
- Google Cloud Storage: https://docs.launchflow.com/buildflow/primitives/gcp/gcs

    from buildflow.io.gcp import GCSBucket

    bucket = GCSBucket(project_id="project", bucket_name="bucket")

- Amazon S3: https://docs.launchflow.com/buildflow/primitives/aws/s3


    from buildflow.io.aws import S3Bucket

    bucket = S3Bucket(bucket_name="bucket", aws_region="us-east-1")

Then in main.py you can mark these as managed to have `buildflow apply` create / manage
them.

    app.manage(bucket)
\"\"\"
"""


def hello_world_dependencies_template(app_name) -> str:
    return f"""\
\"\"\"This file can contain all of the dependencies needed by your flow.

For example if you needed a authenticated google user: https://docs.launchflow.com/buildflow/dependencies/auth

    from buildflow.dependencies.auth import AuthenticatedGoogleUserDepBuilder

    AuthenticatedGoogleUserDep = AuthenticatedGoogleUserDepBuilder()

Then in your {app_name}/service.py file you can use
the dependency like so:

    @endpoint("/", method="GET")
    def hello_world_endpoint(user_dep: AuthenticatedGoogleUserDep) -> str:
        return "Hello World!"
\"\"\"
"""


def hello_world_main_template(app_name: str) -> str:
    template = f"""\
\"\"\"Here we define the flow which is our main entry point of our project.

For more information see: https://docs.launchflow.com/buildflow/programming-guide/flows
\"\"\"

from buildflow import Flow, FlowOptions
from {app_name}.service import service
from {app_name}.settings import env

# Your Flow is the container for your application.
app = Flow(flow_options=FlowOptions(stack=env.env.value))

app.add_service(service)
"""
    return template


def hello_world_service_template(app_name: str) -> str:
    template = f"""\
\"\"\"Here we define the Service which exposes the endpoints for our application.

For more information see: https://docs.launchflow.com/buildflow/programming-guide/endpoints
\"\"\"

from buildflow import Service

from {app_name}.settings import env

# Define a Service object with a specific ID, all endpoints in this service
# will share the same resources (1 cpu since we didn't specify).
# https://docs.launchflow.com/buildflow/programming-guide/endpoints#service-options
service = Service(service_id="{app_name}-service")


# Add our hello world endpoint to the service.
@service.endpoint("/", method="GET")
def hello_world_endpoint() -> str:
    return f"Hello, from {{env.env.value}}!"
"""
    return template


def hello_world_readme_template(app_name: str, app_dir: str) -> str:
    template = f"""\
# {app_name}

Welcome to BuildFlow!

This is a simple example of serving the string "Hello World!" from a service [endpoint](https://docs.launchflow.com/buildflow/programming-guide/endpoints)

If you want to get started quickly you can run your application with:

```
buildflow run
```

Then you can visit http://localhost:8000 to see your application running.

## Directory Structure

At the root level there are three important files:

- `buildflow.yml` - This is the main configuration file for your application. It contains all the information about your application and how it should be built.
- `main.py` - This is the entry point to your application and where your `Flow` is initialized.
- `requirements.txt` - This is where you can specify any Python dependencies your application has.

Below the root level we have:

**{app_name}**

This is the directory where your application code lives. You can put any files you want in here and they will be available to your application. We create a couple directories and files for you:

- **processors**: This is where you can put any custom processors you want to use in your application. In here you will see we have defined a _service.py_ for a service in your application and a _hello_world.py_ file for a custom [endpoint](https://docs.launchflow.com/buildflow/programming-guide/endpoints) processor.
- **primitives.py**: This is where you can define any custom [primitive](https://docs.launchflow.com/buildflow/programming-guide/primitives) resources that your application will need. Note it is empty right now since your initial application is so simple.
- **dependencies.py**: This is where you can define any custom [dependencies](https://docs.launchflow.com/buildflow/programming-guide/dependencies) you might need.
- **settings.py**: This file loads in our environment variables and makes them available to our application.

**.buildflow**

This is a hidden directory that contains all the build artifacts for your application. You can general ignore this directory and it will be automatically generated for you. If you are using github you probably want to put this in your _.gitignore_ file.

## Customizing your application

You application is pretty simple now but you can customize it to do anything you want. Here are some ideas:

- Attach [primitives](https://docs.launchflow.com/buildflow/programming-guide/primitives) to your application to add resources like [databases](https://docs.launchflow.com/buildflow/primitives/gcp/cloud_sql), [queues](https://docs.launchflow.com/buildflow/primitives/aws/sqs), [buckets](https://docs.launchflow.com/buildflow/primitives/aws/s3), or [data warehouses](https://docs.launchflow.com/buildflow/primitives/gcp/bigquery)
- Use [depedencies](https://docs.launchflow.com/buildflow/programming-guide/dependencies) to attach dependencies to your processors. Such as [google auth](https://docs.launchflow.com/buildflow/dependencies/auth#authenticated-google-user), [SQLAlchemy Session](https://docs.launchflow.com/buildflow/dependencies/sqlalchemy), or push data to a [sink](https://docs.launchflow.com/buildflow/dependencies/sink)
- Add additional processors for [async processing](https://docs.launchflow.com/buildflow/programming-guide/consumers) or [data ingestion](https://docs.launchflow.com/buildflow/programming-guide/collectors)
- Manage your entire stack with [BuildFlow's pulumi integration](https://docs.launchflow.com/buildflow/programming-guide/buildflow-yaml#pulumi-configure)
"""
    return template
