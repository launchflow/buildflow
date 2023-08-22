import json
import logging
import os
import sys
import warnings
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from importlib.metadata import version
from pprint import pprint
from typing import Any, Dict, List, Optional

import typer

import buildflow
from buildflow.cli import file_gen, utils
from buildflow.config.buildflow_config import BuildFlowConfig
from buildflow.config.cloud_provider_config import CloudProvider, CloudProviderConfig
from buildflow.io import IOPrimitiveOption, list_all_io_primitives

warnings.simplefilter("ignore", UserWarning)
logging.basicConfig(level=logging.ERROR)

BUILDFLOW_HELP = """\
Welcome to the buildflow CLI!

Use the `run` command to run your flows.

Use the `deploy` command to deploy your entire grid.

Use the `plan` command to see what resources will be used by your flows and grids.
"""
app = typer.Typer(help=BUILDFLOW_HELP)

APP_DIR_OPTION = typer.Option(
    "",
    help=(
        "The directory to look for the app in, by adding this to `sys.path` "
        "we default to looking in the directory."
    ),
)


@app.command(help="Run a buildflow flow.")
def run(
    app: str = typer.Argument(..., help="The flow app to run"),
    start_runtime_server: bool = typer.Option(
        False, help="Whether to start the server for the running flow."
    ),
    runtime_server_host: str = typer.Option(
        "127.0.0.1", help="The host to use for the flow server."
    ),
    runtime_server_port: int = typer.Option(
        9653, help="The port to use for the flow server."
    ),
    run_id: Optional[str] = typer.Option(None, help="The run id to use for this run."),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    if isinstance(imported, buildflow.Flow):
        imported.run(
            start_runtime_server=start_runtime_server,
            runtime_server_host=runtime_server_host,
            runtime_server_port=runtime_server_port,
            run_id=run_id,
        )
    else:
        typer.echo(f"{app} is not a buildflow flow.")
        raise typer.Exit(1)


@app.command(help="Refresh all resources used by a buildflow flow or grid")
def refresh(
    app: str = typer.Argument(..., help="The app to refresh"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    if isinstance(imported, (buildflow.Flow)):
        imported.refresh()

    else:
        typer.echo("plan must be run on a flow, or deployment grid")
        typer.Exit(1)


@app.command(help="Output all resources used by a buildflow flow or grid")
def plan(
    app: str = typer.Argument(..., help="The app to plan"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    if isinstance(imported, (buildflow.Flow)):
        imported.plan()

    else:
        typer.echo("plan must be run on a flow, or deployment grid")
        typer.Exit(1)


@app.command(help="Apply all resources used by a buildflow flow or grid")
def apply(
    app: str = typer.Argument(..., help="The app to plan"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    if isinstance(imported, (buildflow.Flow)):
        imported.apply()

    else:
        typer.echo("plan must be run on a flow, or deployment grid")
        typer.Exit(1)


@app.command(help="Destroy all resources used by a buildflow flow or grid")
def destroy(
    app: str = typer.Argument(..., help="The app to plan"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    if isinstance(imported, (buildflow.Flow)):
        imported.destroy()

    else:
        typer.echo("plan must be run on a flow, or deployment grid")
        typer.Exit(1)


@dataclass
class InspectJSON:
    success: bool
    timestamp: str
    inspect_info: Dict[str, Any]

    def print_json(self):
        print(json.dumps(asdict(self)))


@app.command(help="Inspect the Pulumi Stack state of a buildflow flow")
def inspect(
    app: str = typer.Argument(..., help="The app to inspect."),
    app_dir: str = APP_DIR_OPTION,
    as_json: bool = typer.Option(False, help="Whether to print the output as json"),
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    # TODO: Add support for deployment grids
    runtime = datetime.utcnow().isoformat()
    if isinstance(imported, (buildflow.Flow)):
        flow_state = imported.inspect()
        if not as_json:
            typer.echo(f"Fetching stack state for Flow(id={imported.flow_id})...")

            pprint(flow_state.as_json_dict())
        else:
            InspectJSON(
                success=True, timestamp=runtime, inspect_info=flow_state.as_json_dict()
            ).print_json()
    else:
        if not as_json:
            typer.echo("inspect-stack must be run on a flow")
        else:
            InspectJSON(success=False, timestamp=runtime, inspect_info={}).print_json()
        typer.Exit(1)


_DEFAULT_REQUIREMENTS_TXT = """\
buildflow
"""


@app.command(help="Initialize a new buildflow app")
def init(
    directory: str = typer.Option(default=".", help="The directory to initialize"),
    default_cloud_provider: Optional[CloudProvider] = typer.Option(
        None,
        help="The default cloud provider to use",
    ),
    processor: Optional[str] = typer.Option(
        None, help="The type of Processor to create", hidden=True
    ),
    source: Optional[str] = typer.Option(
        None, help="The IO primitive (class name) to use as a Source", hidden=True
    ),
    sink: Optional[str] = typer.Option(
        None, help="The IO primitive (class name) to use as a Sink", hidden=True
    ),
    default_gcp_project: Optional[str] = typer.Option(
        None, help="The default GCP project to use", hidden=True
    ),
    skip_requirements_file: bool = typer.Option(
        default=False, help="Skip requirements file creation", hidden=True
    ),
):
    buildflow_config_dir = os.path.join(directory, ".buildflow")
    if os.path.exists(buildflow_config_dir):
        typer.echo(
            f"buildflow config already exists at {buildflow_config_dir}, skipping."
        )
        raise typer.Exit(1)
    buildflow_config = BuildFlowConfig.default(
        buildflow_config_dir=buildflow_config_dir
    )

    cloud_provider_config = CloudProviderConfig.default()
    if default_cloud_provider is not None:
        cloud_provider_config.default_cloud_provider = default_cloud_provider
    else:
        options = [option.value for option in CloudProvider]
        user_input = input(f"What is your default cloud provider? {options}: ")
        while user_input not in options:
            user_input = input(f"invalid option, try again {options}: ")

        cloud_provider_config.default_cloud_provider = CloudProvider(user_input)

    if default_gcp_project is not None:
        cloud_provider_config.gcp_options.default_project_id = default_gcp_project
    else:
        try:
            from google.auth import default

            _, project_id = default()
            if project_id:
                cloud_provider_config.gcp_options.default_project_id = project_id
        except:  # noqa: E722
            pass

    buildflow_config.cloud_provider_config = cloud_provider_config

    if not skip_requirements_file:
        requirements_txt_path = os.path.join(directory, "requirements.txt")
        if os.path.exists(requirements_txt_path):
            typer.echo(
                f"requirements.txt already exists at {requirements_txt_path}, skipping."
            )
        else:
            with open(requirements_txt_path, "w") as requirements_txt_file:
                requirements_txt_file.write(_DEFAULT_REQUIREMENTS_TXT)

    buildflow_config.dump(buildflow_config_dir)

    if source is not None and sink is not None:
        file_name = "main"
        file_path = os.path.join(directory, f"{file_name}.py")

        if processor is None:
            file_template = ""
        elif processor == "pipeline":
            file_template = file_gen.generate_pipeline_template(source, sink, file_name)
        elif processor == "collector":
            file_template = file_gen.generate_collector_template(sink, file_name)
        elif processor == "endpoint":
            file_template = file_gen.generate_endpoint_template(file_name)

        if os.path.exists(file_path):
            typer.echo(f"{file_path} already exists, skipping.")
        else:
            with open(file_path, "w") as file:
                file.write(file_template)
    else:
        typer.echo(
            "NOTE: You can generate a main.py file by running `buildflow init --source "
            "<source_class_name> --sink <sink_class_name>`"
        )


@dataclass
class BuildFlowInfo:
    version: str
    # module name -> [class name, ...]  i.e. gcp -> [GCSBucket, ...]
    sources: Dict[str, List[str]]
    sinks: Dict[str, List[str]]
    python_version: str
    ray_version: str
    pulumi_version: str

    @classmethod
    def from_primitives(cls, primitives: List[IOPrimitiveOption]) -> "BuildFlowInfo":
        sources: Dict[str, List[str]] = defaultdict(list)
        sinks: Dict[str, List[str]] = defaultdict(list)
        for primitive in primitives:
            if primitive.source:
                sources[primitive.module_name].append(primitive.class_name)
            if primitive.sink:
                sinks[primitive.module_name].append(primitive.class_name)
        return cls(
            version=version("buildflow"),
            sources=sources,
            sinks=sinks,
            python_version=sys.version.split()[0],
            ray_version=version("ray"),
            pulumi_version=version("pulumi"),
        )

    def print_json(self):
        print(
            json.dumps(
                {
                    "version": self.version,
                    "sources": self.sources,
                    "sinks": self.sinks,
                    "python_version": self.python_version,
                    "ray_version": self.ray_version,
                    "pulumi_version": self.pulumi_version,
                }
            )
        )


@app.command(help="Output information about the current buildflow version")
def info(
    as_json: bool = typer.Option(False, help="Whether to print the output as json"),
):
    result = list_all_io_primitives()
    buildflow_info = BuildFlowInfo.from_primitives(result)
    if as_json:
        buildflow_info.print_json()
    else:
        typer.echo(f"buildflow version: {version('buildflow')}")
        typer.echo("sources:")
        for module_name, class_names in buildflow_info.sources.items():
            typer.echo(f"  {module_name}:")
            for class_name in class_names:
                typer.echo(f"    {class_name}")
        typer.echo("sinks:")
        for module_name, class_names in buildflow_info.sinks.items():
            typer.echo(f"  {module_name}:")
            for class_name in class_names:
                typer.echo(f"    {class_name}")


def main():
    app()


if __name__ == "__main__":
    main()
