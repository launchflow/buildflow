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


def generate_pipeline_template(
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

    buildflow_imports = [
        "from buildflow import Flow",
        f"from buildflow.io.{source_submodule} import {source_class_name}",
        f"from buildflow.io.{sink_submodule} import {sink_class_name}",
    ]
    buildflow_imports.extend(imports)
    buildflow_imports.sort()
    buildflow_imports_str = "\n".join(buildflow_imports)

    template = f"""\"\"\"Generated by BuildFlow

steps to run:
    {cli_steps_str}
\"\"\"
import dataclasses

{buildflow_imports_str}


@dataclasses.dataclass
class InputSchema:
    TODO: str


@dataclasses.dataclass
class OutputSchema:
    TODO: str


app = Flow()


source = {source_class_name}(
    {source_fields}
).options(
    # TODO: uncomment if you want Pulumi to manage the source's resource(s)
    # managed=True,
)
sink = {sink_class_name}(
    {sink_fields}
).options(
    # TODO: uncomment if you want Pulumi to manage the sink's resource(s)
    # managed=True,
)


# Attach a Pipeline to the Flow
@app.pipeline(source=source, sink=sink)
def my_pipeline(event: InputSchema) -> OutputSchema:
    return OutputSchema(event.TODO)
"""

    template = black.format_str(template, mode=black.Mode())
    return template


def generate_collector_template(
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

    buildflow_imports = [
        "from buildflow import Flow",
        f"from buildflow.io.{sink_submodule} import {sink_class_name}",
    ]
    buildflow_imports.extend(imports)
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
class OutputSchema:
    TODO: str


app = Flow()


sink = {sink_class_name}(
    {sink_fields}
).options(
    # TODO: uncomment if you want Pulumi to manage the sink's resource(s)
    # managed=True,
)


# Attach a Collector to the Flow
@app.collector(route="/", method="POST", sink=sink)
def my_collector(request: RequestSchema) -> OutputSchema:
    return OutputSchema(request.TODO)
"""

    template = black.format_str(template, mode=black.Mode())
    return template


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
