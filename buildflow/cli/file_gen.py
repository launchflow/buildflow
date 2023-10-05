# ruff: noqa
import enum
import warnings
from dataclasses import MISSING, fields, is_dataclass
from importlib import import_module
from typing import Iterable, List, Set, Tuple, Type, Union, get_args, get_origin

import black

from buildflow.io import list_all_io_primitives

warnings.simplefilter("ignore", UserWarning)


def get_io_submodule(class_name: str) -> str:
    io_primitives = list_all_io_primitives()
    class_to_submodule_map = {
        option.class_name: option.module_name for option in io_primitives
    }
    return class_to_submodule_map.get(class_name, "")


def get_class(class_name: str, submodule: str) -> Type:
    module = import_module(f"buildflow.io.{submodule}")
    return getattr(module, class_name)


def value_to_string(value):
    imports = set()
    if isinstance(value, enum.Enum):
        class_name = value.__class__.__name__
        module_name = value.__class__.__module__
        imports.add(f"from {module_name} import {class_name}")
        return f"{class_name}.{value.name}", imports
    elif isinstance(value, tuple):
        tuple_str = "("
        for v in value:
            element_str, element_imports = value_to_string(v)
            tuple_str += element_str + ", "
            imports.update(element_imports)
        tuple_str = tuple_str.rstrip(", ") + ("," if len(value) == 1 else "") + ")"
        return tuple_str, imports
    elif isinstance(value, list):
        list_str = "["
        for v in value:
            element_str, element_imports = value_to_string(v)
            list_str += element_str + ", "
            imports.update(element_imports)
        list_str = list_str.rstrip(", ") + "]"
        return list_str, imports
    else:
        return repr(value), imports


def get_field_default_value(field) -> Tuple[str, str]:
    field_type = field.type
    origin_type = get_origin(field_type)
    args_type = get_args(field_type)
    imports = set()

    if origin_type is Union and type(None) in args_type:
        inner_type = [arg for arg in args_type if arg is not type(None)][0]
        is_optional = True
    else:
        inner_type = field_type
        is_optional = False

    if field.default is None or field.default == MISSING:
        if inner_type is int:
            default_value = "0"
        elif inner_type is float:
            default_value = "0.0"
        elif inner_type is str:
            default_value = '"TODO"'
        elif is_dataclass(inner_type):
            class_name = inner_type.__name__
            if class_name.startswith("from buildflow.io.aws"):
                class_name = "from buildflow.io.aws"
            elif class_name.startswith("from buildflow.io.gcp"):
                class_name = "from buildflow.io.gcp"
            elif class_name.startswith("from buildflow.io.local"):
                class_name = "from buildflow.io.local"
            module_name = inner_type.__module__
            imports.add(f"from {module_name} import {class_name}")
            inner_default_fields = []
            for sub_field in fields(inner_type):
                if sub_field.init:
                    sub_default_value, more_imports = get_field_default_value(sub_field)
                    imports.update(more_imports)
                    inner_default_fields.append(f"{sub_field.name}={sub_default_value}")
            inner_default_value = "\n         ".join(inner_default_fields)
            default_value = f"{inner_type.__name__}({inner_default_value}\n)"
        else:
            default_value = '"TODO"'  # Default case for other types
        if origin_type == Iterable or origin_type == List:
            default_value = f"[{default_value}]"
        if origin_type == Set:
            default_value = f"{{{default_value}}}"

    else:
        default_value, more_imports = value_to_string(field.default)
        imports.update(more_imports)
        # Has a default value so is optional.
        is_optional = True

    default_value += ","

    if is_optional:
        default_value += " # Optional field"

    return default_value, imports


def generate_consumer_template(
    project_folder: str,
    source_class_name: str,
    sink_class_name: str,
    file_name: str = "main",
) -> str:
    source_submodule = get_io_submodule(source_class_name)
    sink_submodule = get_io_submodule(sink_class_name)

    source_class = get_class(source_class_name, source_submodule)
    sink_class = get_class(sink_class_name, sink_submodule)

    cloud_providers = set()
    if "gcp" in source_submodule or "gcp" in sink_submodule:
        cloud_providers.add("gcp")
    if "aws" in source_submodule or "aws" in sink_submodule:
        cloud_providers.add("aws")
    if "snowflake" in source_submodule or "snowflake" in sink_submodule:
        cloud_providers.add("snowflake")

    cli_steps = [
        "pip install .",
        "gcloud auth application-default login" if "gcp" in cloud_providers else None,
        "aws sso login --profile <profile_name>" if "aws" in cloud_providers else None,
        f"buildflow run {file_name}:app",
    ]

    cli_steps_str = "\n    ".join(step for step in cli_steps if step is not None)

    imports = set()
    sink_default_values = []
    for field in fields(source_class):
        if field.init:
            default_value, more_imports = get_field_default_value(field)
            imports.update(more_imports)
            sink_default_values.append(f"{field.name}={default_value}")
    source_fields = "\n    ".join(sink_default_values)

    source_default_values = []
    for field in fields(sink_class):
        if field.init:
            default_value, more_imports = get_field_default_value(field)
            imports.update(more_imports)
            source_default_values.append(f"{field.name}={default_value}")
    sink_fields = "\n    ".join(source_default_values)

    buildflow_resource_imports = [
        f"from buildflow.io.{source_submodule} import {source_class_name}",
        f"from buildflow.io.{sink_submodule} import {sink_class_name}",
    ]
    buildflow_resource_imports.extend(imports)
    buildflow_resource_imports.sort()
    buildflow_resource_imports_str = "\n".join(buildflow_resource_imports)

    processor_template = f"""\
import dataclasses

from buildflow import consumer

from {project_folder}.resources.consumer_resources import source, sink

@dataclasses.dataclass
class InputSchema:
    TODO: str


@dataclasses.dataclass
class OutputSchema:
    TODO: str


@consumer(source=source, sink=sink)
def my_consumer(event: InputSchema) -> OutputSchema:
    return OutputSchema(event.TODO)
"""

    resource_template = f"""\
{buildflow_resource_imports_str}

source = {source_class_name}(
    {source_fields}
)
sink = {sink_class_name}(
    {sink_fields}
)
"""

    main_template = f"""\"\"\"Generated by BuildFlow

steps to run:
    {cli_steps_str}
\"\"\"
import dataclasses

from buildflow import Flow

from {project_folder}.processors.consumer import my_consumer
from {project_folder}.resources.consumer_resources import source, sink

app = Flow()
# TODO: uncomment if you want pulumi to manage the source and sink resources.
# app.manage(source, sink)

# Attach a Consumer to the Flow
app.add_consumer(my_consumer)
"""

    main_template = black.format_str(main_template, mode=black.Mode())
    return main_template, resource_template, processor_template


def generate_collector_template(
    project_folder: str,
    sink_class_name: str,
    file_name: str = "main",
) -> str:
    sink_submodule = get_io_submodule(sink_class_name)

    sink_class = get_class(sink_class_name, sink_submodule)

    cloud_providers = set()
    if "gcp" in sink_submodule:
        cloud_providers.add("gcp")
    if "aws" in sink_submodule:
        cloud_providers.add("aws")
    if "snowflake" in sink_submodule:
        cloud_providers.add("snowflake")

    cli_steps = [
        "pip install .",
        "gcloud auth application-default login" if "gcp" in cloud_providers else None,
        "aws sso login --profile <profile_name>" if "aws" in cloud_providers else None,
        f"buildflow run {file_name}:app",
    ]

    cli_steps_str = "\n    ".join(step for step in cli_steps if step is not None)

    imports = set()
    sink_default_values = []
    for field in fields(sink_class):
        if field.init:
            default_value, more_imports = get_field_default_value(field)
            imports.update(more_imports)
            sink_default_values.append(f"{field.name}={default_value}")
    sink_fields = "\n    ".join(sink_default_values)

    buildflow_resource_imports = [
        f"from buildflow.io.{sink_submodule} import {sink_class_name}",
    ]
    buildflow_resource_imports.extend(imports)
    buildflow_resource_imports.sort()
    buildflow_resource_imports_str = "\n".join(buildflow_resource_imports)

    processor_template = f"""\
import dataclasses

from buildflow import collector

from {project_folder}.resources.collector_resources import sink

@dataclasses.dataclass
class RequestSchema:
    TODO: str


@dataclasses.dataclass
class OutputSchema:
    TODO: str


@collector(route="/", method="POST", sink=sink)
def my_collector(request: RequestSchema) -> OutputSchema:
    return OutputSchema(request.TODO)
"""
    resource_template = f"""\
{buildflow_resource_imports_str}

sink = {sink_class_name}(
    {sink_fields}
)
"""

    main_temlpate = f"""\"\"\"Generated by BuildFlow

steps to run:
    {cli_steps_str}
\"\"\"
import dataclasses

from buildflow import Flow

from {project_folder}.processors.collector import my_collector
from {project_folder}.resources.collector_resources import sink

app = Flow()
# TODO: uncomment if you want pulumi to manage the sink resources.
# app.manage(sink)

# Attach a Consumer to the Flow
app.add_collector(my_collector)
"""

    main_temlpate = black.format_str(main_temlpate, mode=black.Mode())
    return main_temlpate, resource_template, processor_template


def generate_endpoint_template(file_name: str = "main") -> str:
    cli_steps = [
        "pip install .",
        f"buildflow run {file_name}:app",
    ]

    cli_steps_str = "\n    ".join(step for step in cli_steps if step is not None)

    buildflow_imports = [
        "from buildflow import Flow",
    ]
    buildflow_imports.sort()
    buildflow_imports_str = "\n".join(buildflow_imports)

    template = f"""\"\"\"Generated by BuildFlow

steps to run:
    {cli_steps_str}
\"\"\"
import dataclasses

{buildflow_imports_str}


@dataclasses.dataclass
class RequestSchema:
    TODO: str


@dataclasses.dataclass
class ResponseSchema:
    TODO: str


app = Flow()


# Attach an Endpoint to the Flow
@app.endpoint(route="/", method="POST")
def my_endpoint(request: RequestSchema) -> ResponseSchema:
    return ResponseSchema(request.TODO)
"""

    template = black.format_str(template, mode=black.Mode())
    return template


def empty_template(project_name: str) -> str:
    template = f"""\"\"\"Generated by BuildFlow.\"\"\"
from buildflow import Flow

from {project_name}.processors.service import service


app = Flow()
app.add_service(service)
"""
    return template


def hello_world_service_template(project_name: str) -> str:
    template = f"""from buildflow import Service

from {project_name}.processors.hello_world import hello_world_endpoint

service = Service(service_id="hello-world-service")
service.add_endpoint(hello_world_endpoint)
"""
    return template


def hello_world_endpoint_template() -> str:
    template = """from buildflow import endpoint

@endpoint("/", method="GET")
def hello_world_endpoint() -> str:
    return "Hello World!"
"""
    return template


def hello_world_readme_template(project_name: str, project_dir: str) -> str:
    template = f"""\
# {project_name}

Welcome to BuildFlow!

If you want to just get started quickly you can run start your project with:

```
buildflow run
```

Then you can visit http://localhost:8000 to see your project running.

## Directory Structure

At the root level there are three important files:

- `buildflow.yml` - This is the main configuration file for your project. It contains all the information about your project and how it should be built.
- `main.py` - This is the entry point to your project and where your `Flow` is initialized.
- `requirements.txt` - This is where you can specify any Python dependencies your project has.

Below the root level we have:

**{project_dir}**

This is the directory where your project code lives. You can put any files you want in here and they will be available to your project. We create a couple directories for you:

- **processors**: This is where you can put any custom processors you want to use in your project. In here you will see we have defined a *service.py* for a service in your project and a *hello_world.py* file for a custom endpoint processor.
- **resources**: This is where you can define any custom primitive resources that your project will need. Note it is empty right now since your initial project is so simple.

**.buildflow**

This is a hidden directory that contains all the build artifacts for your project. You can general ignore this directory and it will be automatically generated for you. If you are using github you probably want to put this in your *.gitignore* file.
"""
    return template
