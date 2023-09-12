import time
from dataclasses import dataclass

from buildflow import Flow
from buildflow.dependencies import global_scoped, process_scoped, replica_scoped


@dataclass
class InputRequest:
    val: int


@dataclass
class OuptutResponse:
    val: int


app = Flow()
service = app.service()


@global_scoped
class GlobalScoped:
    def __init__(self):
        print("DO NOT SUBMIT Initializing global scoped")
        self.global_scoped = "global scoped"


@replica_scoped
class Model:
    def __init__(self, global_scoped: GlobalScoped):
        print("DO NOT SUBMIT global scoped: ", global_scoped.global_scoped)
        print("DO NOT SUBMIT Initializing model")
        self.model = "model"


@process_scoped
class RandomString:
    def __init__(self, model: Model):
        print("DO NOT SUBMIT random string model")
        self.random_string = str(time.time()) + " " + model.model


@service.endpoint(route="/", method="POST")
async def my_endpoint_processor(
    input: InputRequest,
    model: Model,
    random_string: RandomString,
) -> OuptutResponse:
    return OuptutResponse(val=input.val + 1)
