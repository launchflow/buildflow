ProjectID = str


class ProjectAPI:
    project_id: ProjectID

    @classmethod
    def create(cls, project_dir: str) -> "ProjectAPI":
        """Creates a new project in the given directory."""
        raise NotImplementedError("create not implemented")

    @classmethod
    def load(cls, project_dir: str) -> "ProjectAPI":
        """Loads the project from the given directory."""
        raise NotImplementedError("load not implemented")
