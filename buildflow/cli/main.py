import sys

import typer

import buildflow
from buildflow.cli import utils

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
    disable_usage_stats: bool = typer.Option(
        False, help="Disable buildflow usage stats"
    ),
    start_flow_server: bool = typer.Option(
        False, help="Whether to start the server for the running flow."
    ),
    flow_server_host: str = typer.Option(
        "127.0.0.1", help="The host to use for the flow server."
    ),
    flow_server_port: int = typer.Option(
        9653, help="The port to use for the flow server."
    ),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    if isinstance(imported, buildflow.Flow):
        imported.run(
            disable_usage_stats=disable_usage_stats,
            start_runtime_server=start_flow_server,
            runtime_server_host=flow_server_host,
            runtime_server_port=flow_server_port,
        )
    else:
        typer.echo(f"{app} is not a buildflow flow.")
        raise typer.Exit(1)


# @app.command(help="Deploy a buildflow grid.")
# def deploy(
#     app: str = typer.Argument(..., help="The grid app to run"),
#     disable_usage_stats: bool = typer.Option(
#         False, help="Disable buildflow usage stats"
#     ),
#     disable_resource_creation: bool = typer.Option(
#         False, help="Disable resource creation"
#     ),
#     app_dir: str = APP_DIR_OPTION,
# ):
#     sys.path.insert(0, app_dir)
#     imported = utils.import_from_string(app)
#     if isinstance(imported, buildflow.DeploymentGrid):
#         imported.deploy(
#             disable_usage_stats=disable_usage_stats,
#             disable_resource_creation=disable_resource_creation,
#         )
#     else:
#         typer.echo(f"{app} is not a buildflow flow.")
#         raise typer.Exit(1)


@app.command(help="Output all resources used by a buildflow flow or grid")
def plan(
    app: str = typer.Argument(..., help="The app to plan"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    # TODO: Add support for deployment grids
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
    # TODO: Add support for deployment grids
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
    # TODO: Add support for deployment grids
    if isinstance(imported, (buildflow.Flow)):
        imported.destroy()

    else:
        typer.echo("plan must be run on a flow, or deployment grid")
        typer.Exit(1)


@app.command(help="Inspect the Pulumi Stack state of a buildflow flow")
def inspect_stack(
    app: str = typer.Argument(..., help="The app to fetch the stack state of"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    # TODO: Add support for deployment grids
    if isinstance(imported, (buildflow.Flow)):
        typer.echo(f"Fetching stack state for Flow(id={imported.flow_id})...")
        pulumi_stack_state = imported.get_pulumi_stack_state()
        pulumi_stack_state.print_summary()

    else:
        typer.echo("inspect-stack must be run on a flow")
        typer.Exit(1)


def main():
    app()


if __name__ == "__main__":
    main()
