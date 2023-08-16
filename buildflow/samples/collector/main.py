from dataclasses import dataclass

from buildflow import Flow
from buildflow.io.gcp import BigQueryTable


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
    sink=BigQueryTable(
        project_id="caleb-launchflow-sandbox",
        dataset_name="collector_testing",
        table_name="my_table",
    ).options(managed=True),
)
def process(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)
