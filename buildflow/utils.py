import dataclasses
import datetime
import inspect
import json
import logging
import os
from typing import Any, Optional, TypeVar
from uuid import uuid4
import hashlib

import requests

# create a UUID type alias
# NOTE: python 3.8 doesn't support typing.TypeAlias
UUID: str


def stable_hash(obj: Any):
    """Creates a hash thats stable across runs."""
    return hashlib.sha256(str(obj).encode("utf-8")).hexdigest()


def uuid(max_len: Optional[int] = None) -> str:
    if max_len is not None:
        return str(uuid4())[:max_len]
    return str(uuid4())


def timestamp_millis() -> int:
    return int(datetime.datetime.now().timestamp() * 1000)


def get_fn_args(fn) -> inspect.FullArgSpec:
    return inspect.getfullargspec(fn)


def log_errors(endpoint: str):
    logging.debug("log_errors not implemented")

    def decorator_function(original_function):
        return original_function

    return decorator_function


# TODO: reconcile the session file with more general `WorkspaceAPI` of sorts
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
            session = Session(id=uuid())
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


T = TypeVar("T")
