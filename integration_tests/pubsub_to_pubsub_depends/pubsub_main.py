import dataclasses

import buildflow

app = buildflow.Node()


@dataclasses.dataclass
class Output:
    output_val: int


@dataclasses.dataclass
class Input:
    val: int


@app.processor(
    source=buildflow.PubSubSource(
        cloud="gcp", name="p2-source", project_id="pubsub-test-project"
    ),
    sink=buildflow.PubSubSink(
        cloud="gcp", name="final_output", project_id="pubsub-test-project"
    ),
)
def processor2(payload: Output):
    return Output(output_val=payload.output_val + 1)


@app.processor(
    source=buildflow.PubSubSource(
        cloud="gcp", name="p1-source", project_id="pubsub-test-project"
    )
)
def processor1(
    payload: Input,
    p1: buildflow.PubSub[Output] = buildflow.Depends(processor2.source()),
):
    p1.publish(Output(payload.val + 1))
