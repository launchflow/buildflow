from buildflow.core.runner.state.flow_state import FlowState
from buildflow.core.app.flow import Flow
from buildflow.core.runner.server import Server
from buildflow import utils
import inspect
import os
from ray import serve
import asyncio
import logging


def _get_directory_path_of_caller():
    # NOTE: This function is used to get the file path of the caller of the
    # Node(). This is used to determine the ProjectWorkspace directory.
    frame = inspect.stack()[2]
    module = inspect.getmodule(frame[0])
    return os.path.dirname(os.path.abspath(module.__file__))


# NEXT TIME:
#  - move server into Flow
#  - move debug_run into Flow
#  - Runner should not block by default. It should return so user can run drain manually
#  - Runner will need a way to look up a running flow maybe


class Runner:
    def run(
        self,
        flow: Flow,
        *,
        disable_usage_stats: bool = False,
        block: bool = True,
        # move into flow options
        debug_run: bool = False,
        # make these flow options and move server into flow
        start_server: bool = False,
        server_host: str = "127.0.0.1",
        server_port: int = 9653,
    ):
        # BuildFlow Usage Stats
        if not disable_usage_stats:
            utils.log_buildflow_usage()

        # Run the flow
        asyncio.get_event_loop().run_until_complete(flow.run())

        # Runner Server / UI
        if start_server:
            server = Server.bind(runtime_actor=flow._runtime_actor, infra_actor=None)
            serve.run(
                server,
                host=server_host,
                port=server_port,
            )
            server_log_message = (
                "-" * 80
                + "\n\n"
                + f"Flow Server running at http://{server_host}:{server_port}\n\n"
                + "-" * 80
                + "\n\n"
            )
            logging.info(server_log_message)
            print(server_log_message)

        coro = flow.run_until_complete()
        if block:
            asyncio.get_event_loop().run_until_complete(coro)
        else:
            return coro

        # if save_state:
        #     runtime_future: asyncio.Future = asyncio.wrap_future(coro.future())
        #     # schedule the destroy to run after the runtime is done.
        #     # NOTE: This will only run if the runtime finishes successfully.
        #     runtime_future.add_done_callback(lambda _: self._save_state())

    def plan(self, flow: Flow):
        return asyncio.get_event_loop().run_until_complete(flow.plan())

    def apply(self, flow: Flow):
        return asyncio.get_event_loop().run_until_complete(flow.apply())

    def destroy(self, flow: Flow):
        return asyncio.get_event_loop().run_until_complete(flow.destroy())

    def state(self) -> FlowState:
        """Returns the current state of the buildflow application."""
        raise NotImplementedError("state not implemented")
