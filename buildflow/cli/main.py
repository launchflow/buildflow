import sys

import typer

import buildflow
from buildflow.core.app.flow import Flow
from buildflow.cli import utils

BUILDFLOW_HELP = """\
Welcome to the buildflow CLI!

Use the `run` command to run your buildflow nodes.

Use the `deploy` command to deploy your entire grid.

Use the `plan` command to see what resources will be used by your nodes and grids.
"""
app = typer.Typer(help=BUILDFLOW_HELP)

APP_DIR_OPTION = typer.Option(
    "",
    help=(
        "The directory to look for the app in, by adding this to `sys.path` "
        "we default to looking in the directory."
    ),
)


@app.command(help="Run a buildflow node.")
def run(
    app: str = typer.Argument(..., help="The node app to run"),
    disable_usage_stats: bool = typer.Option(
        False, help="Disable buildflow usage stats"
    ),
    apply_infrastructure: bool = typer.Option(
        False, help="Whether resources should be created"
    ),
    destroy_infrastructure: bool = typer.Option(
        False, help="Whether resources should be destroyed."
    ),
    start_node_server: bool = typer.Option(
        False, help="Whether to start the server for the running node."
    ),
    node_server_host: str = typer.Option(
        "127.0.0.1", help="The host to use for the node server."
    ),
    node_server_port: int = typer.Option(
        9653, help="The port to use for the node server."
    ),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    if isinstance(imported, Flow):
        imported.run(
            disable_usage_stats=disable_usage_stats,
            start_runtime_server=start_node_server,
            runtime_server_host=node_server_host,
            runtime_server_port=node_server_port,
        )
    else:
        typer.echo(f"{app} is not a buildflow node.")
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
#         typer.echo(f"{app} is not a buildflow node.")
#         raise typer.Exit(1)


@app.command(help="Output all resources used by a buildflow node or grid")
def plan(
    app: str = typer.Argument(..., help="The app to plan"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    # TODO: Add support for deployment grids
    if isinstance(imported, (buildflow.Node)):
        imported.plan()
        # print(plan)
        # print()
        # user_input = ""
        # while True:
        #     user_input = input(
        #         "Would you like to setup the resources for this plan (Y/n)? "
        #     )
        #     if user_input.lower() not in ["y", "n"]:
        #         print('Please enter "y" or "n"')
        #     else:
        #         break

        # if user_input == "n":
        #     return
        # imported.apply()

    else:
        typer.echo("plan must be run on a node, or deployment grid")
        typer.Exit(1)


@app.command(help="Apply all resources used by a buildflow node or grid")
def apply(
    app: str = typer.Argument(..., help="The app to plan"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    # TODO: Add support for deployment grids
    if isinstance(imported, (buildflow.Node)):
        plan = imported.plan()
        print(plan)
        print()
        imported.apply()

    else:
        typer.echo("plan must be run on a node, or deployment grid")
        typer.Exit(1)


@app.command(help="Destroy all resources used by a buildflow node or grid")
def destroy(
    app: str = typer.Argument(..., help="The app to plan"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    # TODO: Add support for deployment grids
    if isinstance(imported, (buildflow.Node)):
        imported.destroy()

    else:
        typer.echo("plan must be run on a node, or deployment grid")
        typer.Exit(1)


def main():
    app()


if __name__ == "__main__":
    main()
