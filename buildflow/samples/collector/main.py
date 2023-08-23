from dataclasses import dataclass

from buildflow import Flow
from buildflow.io.gcp import CloudSQLDatabase, CloudSQLInstance
from buildflow.io.postgres import PostgresTable

instance = CloudSQLInstance(
    instance_name="my-instance", project_id="caleb-launchflow-sandbox"
).options(managed=True)
database = CloudSQLDatabase(database_name="testing", instance=instance).options(
    managed=True
)


@dataclass
class InputRequest:
    val: int


@dataclass
class OuptutResponse:
    val: int


app = Flow()


@app.collector(
    route="/",
    method="POST",
    sink=PostgresTable(table="table", database=database).options(managed=True),
)
def process(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)
