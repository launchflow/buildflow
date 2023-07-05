import dataclasses

from buildflow.core.io.resources._resource import Resource
from buildflow.core.options.resource_options import ResourceOptions
from buildflow import utils


@dataclasses.dataclass
class BigQueryTable(Resource):
    table_id: str
    exclude_from_infra: bool = False

    @classmethod
    def from_options(cls, resource_options: ResourceOptions) -> "Resource":
        project_id = resource_options.gcp.default_project_id
        project_hash = utils.stable_hash(project_id)
        table_name = f"table_{project_hash[:8]}"
        table_id = f"{project_id}.buildflow.{table_name}"
        return cls(table_id=table_id)
