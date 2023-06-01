import dataclasses
import json
import logging
import os

import requests

from buildflow import utils

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


def log_buildflow_usage():
    session = _load_buildflow_session()
    logging.debug(
        "Usage stats collection is enabled. To disable set "
        "`disable_usage_stats` in flow.run() or set the environment "
        "variable BUILDFLOW_USAGE_STATS_DISABLE."
    )
    response = requests.post(
        "https://apis.launchflow.com/buildflow_usage",
        data=json.dumps(dataclasses.asdict(session)),
    )
    if response.status_code == 200:
        logging.debug("recorded run in session %s", session)
    else:
        logging.debug("failed to record usage stats.")
