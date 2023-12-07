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
from typing import Any, Callable, Dict, List, Optional

import pathspec
import ray
import typer
import yaml
from rich.progress import Progress, SpinnerColumn, TextColumn

import buildflow
from buildflow.cli import file_gen, utils
from buildflow.cli.watcher import RunTimeWatcher
from buildflow.config.buildflow_config import BUILDFLOW_CONFIG_FILE, BuildFlowConfig
from buildflow.core.app.flow_state import FlowState
from buildflow.core.utils import uuid

warnings.simplefilter("ignore", UserWarning)
logging.basicConfig(level=logging.ERROR)

_APP_NAME_PATTERN = r"^[a-z-]{1,63}$"
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
    serve_host: str,
    serve_port: int,
    flow_state: Optional[FlowState] = None,
    event_subscriber: Optional[Callable] = None,
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
                serve_host=serve_host,
                serve_port=serve_port,
                event_subscriber=event_subscriber,
            )
            asyncio.run(watcher.run())

        else:
            flow.run(
                start_runtime_server=start_runtime_server,
                runtime_server_host=runtime_server_host,
                runtime_server_port=runtime_server_port,
                run_id=run_id,
                flow_state=flow_state,
                serve_host=serve_host,
                serve_port=serve_port,
                event_subscriber=event_subscriber,
            )
    else:
        typer.echo(f"{app} is not a buildflow.Flow object.")
        raise typer.Exit(1)


@app.command(help="Run a buildflow application.")
def run(
    serve_host: str = typer.Option(
        "127.0.0.1", help="The host to use for serving endpoints and collectors."
    ),
    serve_port: int = typer.Option(
        8000, help="The port to use for serving endpoitns and collectors."
    ),
    start_runtime_server: bool = typer.Option(
        False, help="Whether to start the server for the running flow."
    ),
    runtime_server_host: str = typer.Option(
        "127.0.0.1", help="The host to use for the runtime server."
    ),
    runtime_server_port: int = typer.Option(
        9653, help="The port to use for the runtime server."
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
    event_subscriber: str = typer.Option(
        None,
        help="A python function that will be called when a status change occurs in the runtime.",  # noqa
    ),
):
    event_subscriber_import = None
    if event_subscriber:
        sys.path.insert(0, "")
        event_subscriber_import = utils.import_from_string(event_subscriber)
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
            serve_host,
            serve_port,
            event_subscriber=event_subscriber_import,
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
            envent_subscriber=event_subscriber_import,
        )


def zipdir(path: str, ziph: zipfile.ZipFile, excludes: List[str]):
    # TODO: Move checks like this to a dedicated validation module
    requirements_file_found = False
    # ziph is zipfile handle
    file_names = []
    matcher = pathspec.PathSpec.from_lines("gitwildmatch", excludes)
    for root, dirs, files in os.walk(path):
        for file in files:
            if file == "requirements.txt":
                requirements_file_found = True
            base_path = os.path.join(root, file)
            if matcher.match_file(base_path):
                continue
            file_names.append(base_path)

    if not requirements_file_found:
        typer.echo(
            "Build failed: requirements.txt not found.\n"
            "Please add one to the root directory and try again."
        )
        raise typer.Exit(1)

    for file in file_names:
        ziph.write(
            file,
            os.path.relpath(file, path),
        )


_BASE_EXCLUDE = [".buildflow", "__pycache__", "build", ".git", ".gitignore"]
_BUILD_PATH_HELP = "Output directory to store the build in, defaults to ./.buildflow/build/build.flow"  # noqa


@app.command(help="Build a buildflow application.")
def build(
    build_path: str = typer.Option("", help=_BUILD_PATH_HELP),
    ignores: List[str] = typer.Option(
        [],
        help="Files to ignore when building, these can be the same syntax as .gitignore",  # noqa
    ),  # noqa
    as_template: bool = typer.Option(
        False,
        help="Whether to output the build as a template, this will not include the flowstate.yaml file.",  # noqa
    ),
):
    buildflow_config = BuildFlowConfig.load()
    sys.path.insert(0, "")
    imported = utils.import_from_string(buildflow_config.entry_point)
    if not build_path:
        if as_template:
            build_path = os.path.join(
                os.getcwd(), ".buildflow", "build", "template.flow"
            )
        else:
            build_path = os.path.join(os.getcwd(), ".buildflow", "build", "build.flow")
    build_path = os.path.abspath(build_path)
    path = pathlib.Path(build_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    final_excludes = _BASE_EXCLUDE + ignores + buildflow_config.build_ignores
    if as_template:
        # TODO: Clear any private fields in the buildflow.yaml
        pass
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


@app.command(help="Refresh all resources used by a buildflow application")
def refresh():
    buildflow_config = BuildFlowConfig.load()
    sys.path.insert(0, "")
    imported = utils.import_from_string(buildflow_config.entry_point)
    if isinstance(imported, (buildflow.Flow)):
        imported.refresh()

    else:
        typer.echo("refresh must be run on a flow")
        typer.Exit(1)


@app.command(help="Output all resources used by a buildflow application")
def preview():
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        load_task = progress.add_task("Loading Application...", total=1)
        buildflow_config = BuildFlowConfig.load()
        sys.path.insert(0, "")
        imported = utils.import_from_string(buildflow_config.entry_point)
        progress.advance(load_task)
        progress.remove_task(load_task)
        progress.console.print("[green]✓[/green] Application Loaded")
        if isinstance(imported, (buildflow.Flow)):
            imported.preview(progress)
        else:
            typer.echo("preview must be run on a flow")
            typer.Exit(1)


@app.command(help="Apply all resources used by a buildflow application")
def apply():
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        load_task = progress.add_task("Loading Application...", total=1)
        buildflow_config = BuildFlowConfig.load()
        sys.path.insert(0, "")
        imported = utils.import_from_string(buildflow_config.entry_point)
        progress.advance(load_task)
        progress.remove_task(load_task)
        progress.console.print("[green]✓[/green] Application Loaded")
        if isinstance(imported, (buildflow.Flow)):
            imported.apply(progress)

        else:
            typer.echo("preview must be run on a flow")
            typer.Exit(1)


@app.command(help="Destroy all resources used by a buildflow application ")
def destroy():
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        load_task = progress.add_task("Loading Application...", total=1)
        buildflow_config = BuildFlowConfig.load()
        sys.path.insert(0, "")
        imported = utils.import_from_string(buildflow_config.entry_point)
        progress.advance(load_task)
        progress.remove_task(load_task)
        progress.console.print("[green]✓[/green] Application Loaded")
        if isinstance(imported, (buildflow.Flow)):
            imported.destroy(progress)

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


@app.command(help="Inspect the Pulumi Stack state of a buildflow application")
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
python-dotenv
"""


@app.command(help="Initialize a new buildflow app")
def init(
    app: str = typer.Option("", help=("The name of the app")),
    directory: str = typer.Option(default=".", help="The directory to initialize"),
):
    buildflow_config_dir = os.path.join(directory, BUILDFLOW_CONFIG_FILE)
    if os.path.exists(buildflow_config_dir):
        typer.echo(
            f"buildflow config already exists at {buildflow_config_dir}, skipping."
        )
        raise typer.Exit(1)
    if not app:
        app = input("Please enter an app name: ")
    if not re.fullmatch(_APP_NAME_PATTERN, app):
        typer.echo("app name must contain only lowercase letters and hyphens")
        raise typer.Exit(1)
    buildflow_config = BuildFlowConfig.default(app=app, directory=directory)

    requirements_txt_path = os.path.join(directory, "requirements.txt")
    if os.path.exists(requirements_txt_path):
        typer.echo(
            f"requirements.txt already exists at {requirements_txt_path}, skipping."
        )
    else:
        with open(requirements_txt_path, "w") as requirements_txt_file:
            requirements_txt_file.write(_DEFAULT_REQUIREMENTS_TXT)

    os.makedirs(directory, exist_ok=True)
    buildflow_config.dump(directory)

    # Create the base app folder
    app_folder = app.replace("-", "_")
    app_lib_dir = os.path.join(directory, app_folder)
    os.mkdir(app_lib_dir)
    open(os.path.join(app_lib_dir, "__init__.py"), "w").close()

    # Create the primitives module
    primitives_template = file_gen.hello_world_primitives_template()
    with open(os.path.join(app_lib_dir, "primitives.py"), "w") as f:
        f.write(primitives_template)

    resources_template = file_gen.hello_world_dependencies_template(app_folder)
    with open(os.path.join(app_lib_dir, "dependencies.py"), "w") as f:
        f.write(resources_template)

    service_template = file_gen.hello_world_service_template(app_folder)
    with open(os.path.join(app_lib_dir, "service.py"), "w") as f:
        f.write(service_template)

    settings_template = file_gen.hello_world_settings_template()
    with open(os.path.join(app_lib_dir, "settings.py"), "w") as f:
        f.write(settings_template)

    file_name = "main"
    file_path = os.path.join(directory, f"{file_name}.py")

    main_file_template = file_gen.hello_world_main_template(app_folder)
    file_name = "main"
    file_path = os.path.join(directory, f"{file_name}.py")
    if os.path.exists(file_path):
        typer.echo(f"{file_path} already exists, skipping.")
    else:
        with open(file_path, "w") as file:
            file.write(main_file_template)

    # Create the readme file
    with open(os.path.join(directory, "README.md"), "w") as f:
        f.write(file_gen.hello_world_readme_template(app, app_folder))


def main():
    app()


if __name__ == "__main__":
    main()
