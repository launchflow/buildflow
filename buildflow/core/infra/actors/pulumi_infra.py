import inspect
import logging
from typing import Iterable

import pulumi
import ray
from pulumi import automation as auto

from buildflow.api import InfraAPI
from buildflow.core.infra.config import InfraConfig
from buildflow.core.processor.base import Processor
from buildflow.io.providers import PulumiProvider
import os


# TODO: reconcile the session file with more general `WorkspaceAPI` of sorts
_PULUMI_DIR = os.path.join(os.path.expanduser("~"), ".config", "buildflow", "pulumi")
os.makedirs(_PULUMI_DIR, exist_ok=True)


@ray.remote
class PulumiInfraActor(InfraAPI):
    def __init__(
        self,
        config: InfraConfig,
        *,
        # TODO: Support more pulumi options
        stack_name: str,
        project_name: str = "buildflow-app",
        passphrase: str = "",
        backend_url: str = f"file://{_PULUMI_DIR}",
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(config.log_level)

        # set configuration
        self.config = config
        self.project_name = project_name
        self.stack_name = stack_name
        # reconcile configuration and env vars into an env dict for Pulumi
        if os.environ.get("PULUMI_CONFIG_PASSPHRASE") is not None:
            if passphrase and passphrase != os.environ.get("PULUMI_CONFIG_PASSPHRASE"):
                raise RuntimeError(
                    "The provided passphrase does not match the "
                    "PULUMI_CONFIG_PASSPHRASE environment variable."
                )
            passphrase = os.environ.get("PULUMI_CONFIG_PASSPHRASE")

        self.workspace_options = auto.LocalWorkspaceOptions(
            env_vars={
                "PULUMI_CONFIG_PASSPHRASE": passphrase,
            },
            stack_settings={
                self.stack_name: auto.StackSettings(
                    config={"gcp:project": self.project_name}
                )
            },
            project_settings=auto.ProjectSettings(
                name=self.project_name,
                runtime="python",
                backend=auto.ProjectBackend(backend_url),
            ),
        )

    async def plan(self, *, processors: Iterable[Processor]):
        logging.debug(f"Planning Pulumi stack: {self.stack_name}")
        stack = self._create_or_select_stack(processors)
        preview_result: auto.PreviewResult = stack.preview()

        # should we log any of this on debug level?

        # log pulumi's stdout and stderr
        logging.debug(f"plan: {preview_result.stdout}")
        if preview_result.stderr:
            logging.error(f"plan: {preview_result.stderr}")

        # TODO: print a more useful change summary
        logging.debug(f"plan: {preview_result.change_summary}")
        return preview_result.stdout

    async def apply(self, *, processors: Iterable[Processor]):
        logging.info(f"Applying Pulumi stack: {self.stack_name}")
        # Planning phase (no remote state changes)
        stack = self._create_or_select_stack(processors)
        preview_result: auto.PreviewResult = stack.preview()
        if self.config.require_confirmation:
            print("Would you like to apply these changes?")
            print(preview_result.change_summary)
            response = input('Enter "yes" to confirm: ')
            if response != "yes":
                print("Aborting.")
                return
            print("Applying...")

        noop_changes_filtered_out = {
            k: v
            for k, v in preview_result.change_summary.items()
            if v != auto.OpType.SAME
        }
        if noop_changes_filtered_out:
            logging.warning(f"apply: Applying Pulumi Stack: {self.stack_name}")
            logging.warning(f"apply: Changes: {noop_changes_filtered_out}")

        # Execution phase (potentially remote state changes)
        up_result: auto.UpResult = stack.up()
        logging.info(f"apply: {up_result.stdout}")
        if up_result.stderr:
            logging.error(f"apply: {up_result.stderr}")
        logging.info(f"apply: {up_result.summary}")
        logging.info(f"apply: {up_result.outputs}")

    async def destroy(self, *, processors: Iterable[Processor]):
        logging.warning(f"destroy: Destroying Pulumi stack: {self.stack_name}")
        # Planning phase (no remote state changes)
        stack = self._create_or_select_stack(processors)
        outputs_map: auto.OutputMap = stack.outputs()
        if self.config.require_confirmation:
            print(f"Would you like to delete this stack ({self.stack_name})?")
            print(outputs_map)
            response = input('Enter "yes" to confirm: ')
            if response != "yes":
                print("Aborting.")
                return

        logging.warning(f"destroy: Removing: {outputs_map}")

        # Execution phase (potentially remote state changes)
        destroy_result: auto.DestroyResult = stack.destroy()
        logging.info(f"destroy: {destroy_result.stdout}")
        if destroy_result.stderr:
            logging.error(f"destroy: {destroy_result.stderr}")
        logging.info(f"destroy: {destroy_result.summary}")

    def _create_pulumi_program(self, processors: Iterable[Processor]):
        def pulumi_program():
            for processor in processors:
                full_arg_spec = inspect.getfullargspec(processor.process)
                output_type = None
                input_type = None
                if "return" in full_arg_spec.annotations:
                    output_type = full_arg_spec.annotations["return"]
                if (
                    len(full_arg_spec.args) > 1
                    and full_arg_spec.args[1] in full_arg_spec.annotations
                ):
                    input_type = full_arg_spec.annotations[full_arg_spec.args[1]]
                # Fetch the Pulumi Resources from the source provider (if any)
                source_provider = processor.source().provider()
                if isinstance(source_provider, PulumiProvider):
                    pulumi_resources = source_provider.pulumi(input_type)
                    logging.debug(f"pulumi_resources: {pulumi_resources}")
                    for key, value in pulumi_resources.exports.items():
                        pulumi.export(key, value)

                # TODO: Add support for .sinks() method on Processor
                # Fetch the Pulumi Resources from the sink provider (if any)
                sink_provider = processor.sink().provider()
                if isinstance(sink_provider, PulumiProvider):
                    pulumi_resources = sink_provider.pulumi(output_type)
                    logging.debug(f"pulumi_resources: {pulumi_resources}")
                    for key, value in pulumi_resources.exports.items():
                        pulumi.export(key, value)

        return pulumi_program

    def _create_or_select_stack(self, processors: Iterable[Processor]):
        pulumi_program = self._create_pulumi_program(processors)

        return auto.create_or_select_stack(
            stack_name=self.stack_name,
            project_name=self.project_name,
            program=pulumi_program,
            opts=self.workspace_options,
        )
