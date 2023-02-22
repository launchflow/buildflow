import buildflow as flow
import inspect


@flow.processor(input_ref=flow.BigQuery(query=''))
def preprocess(payload):
    ...


# @flow.processor(input_ref=preprocess)
# def model_1(payload):
#     ...

# @flow.processor(input_ref=preprocess)
# def model_2(payload):
#     ...

flow.run(interactive=False)
