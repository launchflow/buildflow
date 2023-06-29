from buildflow.api_v2.workspace import WorkspaceAPI


class RunnerAPI:
    workspace: WorkspaceAPI

    def run(self):
        """Runs the buildflow application."""
        raise NotImplementedError("run not implemented")
