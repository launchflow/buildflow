import asyncio
from dataclasses import dataclass

from buildflow import Flow


@dataclass
class InputRequest:
    val: int


@dataclass
class OuptutResponse:
    val: int


app = Flow()
service = app.service()

_model = None


async def load_model():
    global _model
    if _model is None:
        print("DO NOT SUBMIT: LOADING MODEL")
        await asyncio.sleep(5)
        _model = "model"
    return _model


@service.endpoint(route="/", method="POST")
async def my_endpoint_processor(input: InputRequest) -> OuptutResponse:
    print(await load_model())
    return OuptutResponse(val=input.val + 1)


@service.endpoint(route="/diff", method="POST")
async def my_difft_processor(input: InputRequest) -> OuptutResponse:
    print(await load_model())
    return OuptutResponse(val=input.val + 2)


service2 = app.service(base_route="/service2")


@service2.endpoint(route="/", method="POST")
async def my_endpoint_processor2(input: InputRequest) -> OuptutResponse:
    print(await load_model())
    return OuptutResponse(val=input.val + 1)


@service2.endpoint(route="/diff", method="POST")
async def my_difft_processor2(input: InputRequest) -> OuptutResponse:
    print(await load_model())
    return OuptutResponse(val=input.val + 2)
