import sys

import typer

import buildflow
from buildflow.cli import utils

app = typer.Typer()

APP_DIR_OPTION = typer.Option(
    "",
    help=(
        "The directory to look for the app in, by adding this to `sys.path` "
        "we default to looking in the current working directory."
    ),
)


@app.command(help="Run a buildflow node or grid.")
def run(
    app: str = typer.Argument(..., help="The app to run"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    imported = utils.import_from_string(app)
    if isinstance(imported, buildflow.Node):
        imported.run()
    if isinstance(imported, buildflow.DeploymentGrid):
        imported.deploy()


@app.command(help="Output all resources used by a buildflow node or grid")
def plan(
    app: str = typer.Argument(..., help="The app to plan"),
    app_dir: str = APP_DIR_OPTION,
):
    sys.path.insert(0, app_dir)
    raise NotImplementedError("Not implemented yet")


def main():
    app()


if __name__ == "__main__":
    main()
