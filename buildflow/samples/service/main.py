from dataclasses import dataclass

from buildflow import Flow


@dataclass
class InputRequest:
    val: int


@dataclass
class OuptutResponse:
    val: int


app = Flow()


@app.endpoint(route="/", method="POST")
def process(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)


@app.endpoint(route="/two", method="POST")
def process2(input: InputRequest) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)
