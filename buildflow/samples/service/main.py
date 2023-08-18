from dataclasses import dataclass

from buildflow import Flow
from buildflow.io.local import File, Pulse


@dataclass
class InputRequest:
    val: int


@dataclass
class OuptutResponse:
    val: int


app = Flow()


@app.endpoint(route="/", method="POST")
def my_endpoint_processor(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)


@app.collector(route="/two", method="POST", sink=File("output.txt", "csv"))
def my_collector_processor(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)


@app.pipeline(source=Pulse([InputRequest(1)], 1), sink=File("output.txt", "csv"))
def my_pipeline_processor(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)
