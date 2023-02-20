# flake8: noqa
from flowstate.api import *
from flowstate.runtime.decorator import processor
from flowstate.runtime.runner import Runtime


def run():
    Runtime().run()