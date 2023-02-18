import flow_io
from flow_io import ray_io
import ray
import time

config = {
    'input':
    flow_io.PubSub(
        subscription='projects/daring-runway-374503/subscriptions/tanke-test'),
    'outputs': [flow_io.Empty()]
}
flow_io.init(config=config)


@ray.remote
def _process(payload):
    # t1 = time.time()
    # val = [1]
    # while time.time() - t1 < 1.0:
    #     # simulate active work
    #     a = 1 + 1
    #     val[0] = a
    return payload


@ray.remote
class ProcessorActor:

    async def process(self, payload: int):
        print('waiting in process')
        return await _process.remote(payload)


# processor = ProcessorActor.remote()
# ray_io.run(processor.process.remote)

ray_io.run(_process.remote)
