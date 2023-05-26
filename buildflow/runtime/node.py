import dataclasses
import json
import logging
import os
from typing import List

import requests

from buildflow import utils
from buildflow.api import (NodeAPI, NodeApplyResult, NodeDestroyResult,
                           NodePlan, NodeRunResult)
from buildflow.runtime.infrastructure import Infrastructure
from buildflow.runtime.runner import Runtime
from buildflow.runtime.processor import Processor

_SESSION_DIR = os.path.join(os.path.expanduser("~"), ".config", "buildflow")
_SESSION_FILE = os.path.join(_SESSION_DIR, "build_flow_usage.json")


@dataclasses.dataclass
class Session:
    id: str


def _load_buildflow_session():
    try:
        os.makedirs(_SESSION_DIR, exist_ok=True)
        if os.path.exists(_SESSION_FILE):
            with open(_SESSION_FILE, "r") as f:
                session_info = json.load(f)
                return Session(**session_info)
        else:
            session = Session(id=utils.uuid())
            with open(_SESSION_FILE, "w") as f:
                json.dump(dataclasses.asdict(session), f)
            return session
    except Exception as e:
        logging.debug("failed to load session id with error: %s", e)


# TODO: Look for a better home for this function
def _log_buildflow_usage():
    session = _load_buildflow_session()
    print("Usage stats collection is enabled. To disable set "
          "`disable_usage_stats` in flow.run() or set the environment "
          "variable BUILDFLOW_USAGE_STATS_DISABLE.")
    response = requests.post(
        "https://apis.launchflow.com/buildflow_usage",
        data=json.dumps(dataclasses.asdict(session)),
    )
    if response.status_code == 200:
        logging.debug("recorded run in session %s", session)
    else:
        logging.debug("failed to record usage stats.")


class Node(NodeAPI):

    def __init__(self, name: str = "") -> None:
        self.name = name
        self._processors: List[Processor] = []
        # The Node class is a wrapper around the Runtime and Infrastructure
        self._runtime = Runtime()
        self._infrastructure = Infrastructure()

    def add(self, processor: Processor):
        self._processors.append(processor)

    def plan(self) -> NodePlan:
        return self._infrastructure.plan(node_name=self.name,
                                         node_processors=self._processors)

    def run(
        self,
        *,
        disable_usage_stats: bool = False,
        disable_resource_creation: bool = True,
        blocking: bool = True,
    ) -> NodeRunResult:
        # BuildFlow Usage Stats
        if not disable_usage_stats:
            _log_buildflow_usage()
        # BuildFlow Resource Creation
        if not disable_resource_creation:
            self.apply()
        # BuildFlow Runtime
        return self._runtime.run(node_name=self.name,
                                 node_processors=self._processors,
                                 blocking=blocking)

    def apply(self) -> NodeApplyResult:
        print("Setting up resources...")
        result = self._infrastructure.apply(node_name=self.name,
                                            node_processors=self._processors)
        print("...Finished setting up resources")
        return result

    def destroy(self) -> NodeDestroyResult:
        print("Tearing down resources...")
        result = self._infrastructure.destroy(node_name=self.name,
                                              node_processors=self._processors)
        print("...Finished tearing down resources")
        return result
