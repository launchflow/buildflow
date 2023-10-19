import asyncio
import json
import logging
import os
import pathlib
import re
import sys
import tempfile
import warnings
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime
from pprint import pprint
from typing import Any, Dict, List, Optional

import pathspec
import ray
import typer
import yaml

import buildflow
from buildflow.cli import file_gen, utils
from buildflow.cli.watcher import RunTimeWatcher
from buildflow.config.buildflow_config import BUILDFLOW_CONFIG_FILE, BuildFlowConfig
from buildflow.core.app.flow_state import FlowState
from buildflow.core.utils import uuid

warnings.simplefilter("ignore", UserWarning)
logging.basicConfig(level=logging.ERROR)

_PROJECT_NAME_PATTERN = r"^[a-z-]{1,63}$"
BUILDFLOW_HELP = """\
Welcome to the buildflow CLI!
"""
app = typer.Typer(help=BUILDFLOW_HELP, pretty_exceptions_enable=False)

APP_DIR_OPTION = typer.Option(
    "",
    help=(
        "The directory to look for the app in, by adding this to `sys.path` "
        "we default to looking in the working directory."
    ),
)


def run_flow(
    flow: Any,
    buildflow_config: BuildFlowConfig,
    reload: bool,
    run_id: str,
    start_runtime_server: bool,
    runtime_server_host: str,
    runtime_server_port: int,
    flow_state: Optional[FlowState] = None,
):
    if isinstance(flow, buildflow.Flow):
        if reload:
            ray.init()
            watcher = RunTimeWatcher(
                app=buildflow_config.entry_point,
                start_runtime_server=start_runtime_server,
                run_id=run_id,
                runtime_server_host=runtime_server_host,
                runtime_server_port=runtime_server_port,
            )
            asyncio.run(watcher.run())

        else:
            flow.run(
                start_runtime_server=start_runtime_server,
                runtime_server_host=runtime_server_host,
                runtime_server_port=runtime_server_port,
                run_id=run_id,
                flow_state=flow_state,
            )
    else:
        typer.echo(f"{app} is not a buildflow flow.")
        raise typer.Exit(1)


@app.command(help="Run a buildflow flow.")
def run(
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
    reload: bool = typer.Option(False, help="Whether to reload the app on change."),
    from_build: str = typer.Option(
        "", help="The build to run from, only one of app and --from-build can be used."
    ),
    build_output_dir: str = typer.Option(
        "",
        help="The location the build will be decompressed to. Only relevent if setting --from-build, defaults to a temporary directory",  # noqa
    ),
):
    if not from_build:
        buildflow_config = BuildFlowConfig.load()
        sys.path.insert(0, "")
        imported = utils.import_from_string(buildflow_config.entry_point)
        run_flow(
            imported,
            buildflow_config,
            reload,
            run_id,
            start_runtime_server,
            runtime_server_host,
            runtime_server_port,
        )
    else:
        if reload:
            typer.echo("reload cannot be used with --from-build")
            raise typer.Exit(1)
        if not os.path.exists(from_build):
            typer.echo(f"Build at {from_build} does not exist.")
            raise typer.Exit(1)
        if not build_output_dir:
            build_output_dir = os.path.join(tempfile.gettempdir(), "buildflow", uuid())
        with zipfile.ZipFile(from_build, "r") as zip_ref:
            zip_ref.extractall(build_output_dir)
        flowstate_path = os.path.join(build_output_dir, "flowstate.yaml")
        if not os.path.exists(flowstate_path):
            typer.echo(f"Build at {from_build} does not contain a flowstate.yaml.")
            raise typer.Exit(1)
        with open(flowstate_path, "r") as flowstate_file:
            flow_state_yaml = yaml.safe_load(flowstate_file)
        flow_state = FlowState(**flow_state_yaml)
        buildflow_config = BuildFlowConfig.load(build_output_dir)
        sys.path.insert(0, build_output_dir)
        imported = utils.import_from_string(buildflow_config.entry_point)
        run_flow(
            imported,
            buildflow_config,
            reload,
            run_id,
            start_runtime_server,
            runtime_server_host,
            runtime_server_port,
            flow_state=flow_state,
        )


def zipdir(path: str, ziph: zipfile.ZipFile, excludes: List[str]):
    # ziph is zipfile handle
    file_names = []
    matcher = pathspec.PathSpec.from_lines("gitwildmatch", excludes)
    for root, dirs, files in os.walk(path):
        for file in files:
            base_path = os.path.join(root, file)
            if matcher.match_file(base_path):
                continue
            file_names.append(base_path)

    for file in file_names:
        ziph.write(
            file,
            os.path.relpath(file, path),
        )


_BASE_EXCLUDE = [".buildflow", "__pycache__", "build", ".git", ".gitignore"]
_BUILD_PATH_HELP = "Output director to store the build in, defaults to ./.buildflow/build/build.flow"  # noqa


@app.command(help="Build a buildflow flow.")
def build(
    build_path: str = typer.Option("", help=_BUILD_PATH_HELP),
    ignores: List[str] = typer.Option(
        [],
        help="Files to ignore when building, these can be the same syntax as .gitignore",  # noqa
    ),  # noqa
):
    buildflow_config = BuildFlowConfig.load()
    sys.path.insert(0, "")
    imported = utils.import_from_string(buildflow_config.entry_point)
    if not build_path:
        build_path = os.path.join(os.getcwd(), ".buildflow", "build", "build.flow")
    build_path = os.path.abspath(build_path)
    path = pathlib.Path(build_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    final_excludes = _BASE_EXCLUDE + ignores + buildflow_config.build_ignores
    print(f"Generating buildflow build at:\n  {build_path}")
    if isinstance(imported, (buildflow.Flow)):
        flowstate = imported.flowstate()
        with zipfile.ZipFile(build_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipdir(os.getcwd(), zipf, excludes=final_excludes)
            yaml_str = yaml.dump(flowstate.to_dict())
            zipf.writestr("flowstate.yaml", yaml_str)
    else:
        typer.echo("build must be run on a flow")
        typer.Exit(1)


@app.command(help="Refresh all resources used by a buildflow flow")
def refresh():
    buildflow_config = BuildFlowConfig.load()
    sys.path.insert(0, "")
    imported = utils.import_from_string(buildflow_config.entry_point)
    if isinstance(imported, (buildflow.Flow)):
        imported.refresh()

    else:
        typer.echo("refresh must be run on a flow")
        typer.Exit(1)


@app.command(help="Output all resources used by a buildflow flow")
def preview():
    buildflow_config = BuildFlowConfig.load()
    sys.path.insert(0, "")
    imported = utils.import_from_string(buildflow_config.entry_point)
    if isinstance(imported, (buildflow.Flow)):
        imported.preview()

    else:
        typer.echo("preview must be run on a flow")
        typer.Exit(1)


@app.command(help="Apply all resources used by a buildflow flow")
def apply():
    buildflow_config = BuildFlowConfig.load()
    sys.path.insert(0, "")
    imported = utils.import_from_string(buildflow_config.entry_point)
    if isinstance(imported, (buildflow.Flow)):
        imported.apply()

    else:
        typer.echo("preview must be run on a flow")
        typer.Exit(1)


@app.command(help="Destroy all resources used by a buildflow flow ")
def destroy():
    buildflow_config = BuildFlowConfig.load()
    sys.path.insert(0, "")
    imported = utils.import_from_string(buildflow_config.entry_point)
    if isinstance(imported, (buildflow.Flow)):
        imported.destroy()

    else:
        typer.echo("Destroy must be run on a flow")
        typer.Exit(1)


@dataclass
class InspectFlowState:
    success: bool
    timestamp: str
    flowstate: Dict[str, Any]

    def print_json(self):
        print(json.dumps(asdict(self)))


@app.command(help="Inspect the Pulumi Stack state of a buildflow flow")
def inspect(
    as_json: bool = typer.Option(False, help="Whether to print the output as json"),
):
    buildflow_config = BuildFlowConfig.load()
    sys.path.insert(0, "")
    imported = utils.import_from_string(buildflow_config.entry_point)
    # TODO: Add support for deployment s
    runtime = datetime.utcnow().isoformat()
    if isinstance(imported, (buildflow.Flow)):
        flow_state = imported.flowstate()
        if not as_json:
            typer.echo(f"Fetching stack state for Flow(id={imported.flow_id})...")

            pprint(flow_state.as_json_dict())
        else:
            InspectFlowState(
                success=True, timestamp=runtime, flowstate=flow_state.as_json_dict()
            ).print_json()
    else:
        if not as_json:
            typer.echo("inspect-stack must be run on a flow")
        else:
            InspectFlowState(
                success=False, timestamp=runtime, inspect_info={}
            ).print_json()
        typer.Exit(1)


_DEFAULT_REQUIREMENTS_TXT = """\
buildflow
"""


@app.command(help="Initialize a new buildflow app")
def init(
    project: str = typer.Option("", help=("The name of the project")),
    directory: str = typer.Option(default=".", help="The directory to initialize"),
):
    buildflow_config_dir = os.path.join(directory, BUILDFLOW_CONFIG_FILE)
    if os.path.exists(buildflow_config_dir):
        typer.echo(
            f"buildflow config already exists at {buildflow_config_dir}, skipping."
        )
        raise typer.Exit(1)
    if not project:
        project = input("Please enter a project name: ")
    if not re.fullmatch(_PROJECT_NAME_PATTERN, project):
        typer.echo("project must contain only lowercase letters and hyphens")
        raise typer.Exit(1)
    buildflow_config = BuildFlowConfig.default(project=project, directory=directory)

    requirements_txt_path = os.path.join(directory, "requirements.txt")
    if os.path.exists(requirements_txt_path):
        typer.echo(
            f"requirements.txt already exists at {requirements_txt_path}, skipping."
        )
    else:
        with open(requirements_txt_path, "w") as requirements_txt_file:
            requirements_txt_file.write(_DEFAULT_REQUIREMENTS_TXT)

    buildflow_config.dump(directory)

    # Create the base project folder
    project_folder = project.replace("-", "_")
    project_lib_dir = os.path.join(directory, project_folder)
    os.mkdir(project_lib_dir)
    open(os.path.join(project_lib_dir, "__init__.py"), "w").close()

    # Create the primitives module
    open(os.path.join(project_folder, "primitives.py"), "w").close()

    # Create the base processors folder
    processors_folder = os.path.join(project_lib_dir, "processors")
    os.mkdir(processors_folder)
    open(os.path.join(processors_folder, "__init__.py"), "w").close()

    file_name = "main"
    file_path = os.path.join(directory, f"{file_name}.py")

    main_file_template = file_gen.hello_world_main_template(project_folder)
    with open(os.path.join(processors_folder, "service.py"), "w") as f:
        f.write(file_gen.hello_world_service_template(project_folder))

    with open(os.path.join(processors_folder, "hello_world.py"), "w") as f:
        f.write(file_gen.hello_world_endpoint_template())

    # Create the main file
    file_name = "main"
    file_path = os.path.join(directory, f"{file_name}.py")
    if os.path.exists(file_path):
        typer.echo(f"{file_path} already exists, skipping.")
    else:
        with open(file_path, "w") as file:
            file.write(main_file_template)

    # Create the readme file
    with open(os.path.join(directory, "README.md"), "w") as f:
        f.write(file_gen.hello_world_readme_template(project, project_folder))


def main():
    app()


if __name__ == "__main__":
    main()
