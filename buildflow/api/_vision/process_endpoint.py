# Users can connect their processor to an HTTP endpoint.

from dataclasses import dataclass

from tensorflow import keras

from buildflow import Flow
from buildflow.io import HTTPEndpoint

model = keras.models.load_model('path/to/location')

flow = Flow()


@dataclass
class Request:
    prompt: str


@dataclass
class Response:
    prediction: str


# Compare to the CloudRun example in launchflow_provider.py.
@flow.processor(source=HTTPEndpoint(host='localhost', port=3569))
def serve_model(payload: Request) -> Response:
    model_input = preprocess(payload)
    model_output = model.predict(model_input)
    return postprocess(model_output)


def preprocess(payload: Request):
    # TODO: Add logic to preprocess the payload.
    pass


def postprocess(payload) -> Response:
    # TODO: Add logic to postprocess the payload.
    pass
