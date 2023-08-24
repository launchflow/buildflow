from dataclasses import dataclass

from buildflow import Flow
from buildflow.io.gcp import BigQueryDataset, BigQueryTable


@dataclass
class InputRequest:
    val: int


@dataclass
class OuptutResponse:
    val: int


dataset = BigQueryDataset(
    project_id="caleb-launchflow-sandbox", dataset_name="collector_testing2"
).options(managed=True)


app = Flow()


@app.collector(
    route="/",
    method="POST",
    sink=BigQueryTable(dataset=dataset, table_name="my_table").options(managed=True),
)
def process(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)


@app.collector(
    route="/",
    method="POST",
    sink=BigQueryTable(dataset=dataset, table_name="diff_table").options(managed=True),
)
def diff_process2(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)
