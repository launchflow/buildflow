import logging
import os
import sys
from dataclasses import dataclass

import buildflow
from buildflow.io.aws import SQSQueue

app = buildflow.Flow(flow_options=buildflow.FlowOptions(require_confirmation=False))


input_queue = os.getenv("INPUT_QUEUE", "input_queue")
output_queue = os.getenv("OUTPUT_QUEUE", "output_queue")

source = SQSQueue(queue_name=input_queue, aws_region="us-east-1")
sink = SQSQueue(queue_name=output_queue, aws_region="us-east-1")

app.manage(source, sink)


@dataclass
class Input:
    val: int


@dataclass
class Output:
    output_val: int


@app.consumer(source=source, sink=sink, num_cpus=0.5)
class InputConsumer:
    def setup(self):
        self.counter = 0

    def process(self, payload: Input) -> Output:
        self.counter += 1
        if self.counter > 1:
            # Exit with status code 1 to indicate failure
            logging.error("got multiple messages on the input queue")
            sys.exit(1)
        return Output(output_val=payload.val + 1)


@app.consumer(source=sink, num_cpus=0.5)
class ValidateConsumer:
    def setup(self):
        self.counter = 0

    def process(self, payload: Output) -> None:
        self.counter += 1
        if self.counter > 1:
            # Exit with status code 1 to indicate failure
            logging.error("got multiple messages on the output queue")
            sys.exit(1)
        if payload.output_val != 2:
            # Exit with status code 1 to indicate failure
            sys.exit(1)
