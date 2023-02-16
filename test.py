import ray

import flow_io

flow_io.init(
    config={
        'input': flow_io.PubSub(topic='my_type'),
        'outputs': [flow_io.BigQuery(query='SELECT * FROM asdf')]
    })


@ray.remote
class ProcessorActor:

    def __init__(self):
        pass

    def process(self, ray_input, carrier):
        return 1


@ray.remote
def my_func(payload):
    return payload


processor = ProcessorActor.remote()

print(type(processor.process.remote))
print(type(my_func.remote))
