# flake8: noqa
from .runner import Runtime
from .decorator import processor


def run():
    Runtime().run()