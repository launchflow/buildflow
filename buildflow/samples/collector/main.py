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
)
sink1 = BigQueryTable(dataset=dataset, table_name="my_table").options(
    schema=OuptutResponse
)
sink2 = BigQueryTable(dataset=dataset, table_name="diff_table").options(
    schema=OuptutResponse
)


app = Flow()
app.manage(dataset, sink1, sink2)


@app.collector(route="/", method="POST", sink=sink1)
def process(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)


@app.collector(route="/diff", method="POST", sink=sink2)
def diff_process2(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)
