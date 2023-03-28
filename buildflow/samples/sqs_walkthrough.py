# flake8: noqa
import argparse
import dataclasses
from datetime import datetime
import sys
import logging
from typing import Any, Dict

import buildflow
from buildflow import Flow

input_sqs = buildflow.SQSSource(queue_name='caleb-testing2')

flow = Flow()


@flow.processor(source=input_sqs)
def process(element: Dict[str, Any]):
    print('DO NOT SUBMIT: ', element)
    return element


# Run our flow.
flow.run().output()