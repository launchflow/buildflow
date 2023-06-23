import inspect
import logging
from typing import Iterable

import pulumi
import ray
from pulumi import automation as auto

from buildflow.api import InfraAPI
from buildflow.core.infra.config import PulumiWorkspaceConfig, PulumiResourceConfig
from buildflow.core.processor.base import Processor
from buildflow.resources.io.providers import PulumiProvider
import os
import dataclasses


# TODO: reconcile the session file with more general `WorkspaceAPI` of sorts
_PULUMI_DIR = os.path.join(os.path.expanduser("~"), ".config", "buildflow", "pulumi")
os.makedirs(_PULUMI_DIR, exist_ok=True)


@dataclasses.dataclass
class WrappedPreviewResult:
    preview_result: auto.PreviewResult

    def log_summary(self):
        logging.debug(self.preview_result.stdout)
        if self.preview_result.stderr:
            logging.error(self.preview_result.stderr)
        logging.debug(self.preview_result.change_summary)


@dataclasses.dataclass
class WrappedUpResult:
    up_result: auto.UpResult

    def log_summary(self):
        logging.debug(self.up_result.stdout)
        if self.up_result.stderr:
            logging.error(self.up_result.stderr)
        logging.debug(self.up_result.summary)
        logging.debug(self.up_result.outputs)


@dataclasses.dataclass
class WrappedDestroyResult:
    destroy_result: auto.DestroyResult

    def log_summary(self):
        logging.debug(self.destroy_result.stdout)
        if self.destroy_result.stderr:
            logging.error(self.destroy_result.stderr)
        logging.debug(self.destroy_result.summary)


@dataclasses.dataclass
class WrappedOutputMap:
    output_map: auto.OutputMap

    def log_summary(self):
        logging.debug(self.outputs_map)


@ray.remote
class PulumiWorkspaceActor:
    def __init__(
        self,
        config: PulumiWorkspaceConfig,
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(config.log_level)

        # set configuration
        self.config = config
        self.workspace_options = auto.LocalWorkspaceOptions(
            # inline programs do not use work_dir
            # https://www.pulumi.com/docs/reference/pkg/python/pulumi/#automation-api-1
            work_dir=None,
            pulumi_home=self.config.pulumi_home,
            # NOTE: we the program as None here because we will be using the
            # `pulumi_program` method to dynamically create the program from the
            # processors.
            program=None,
            env_vars=self.config.env_vars(),
            # TODO: add support for `secrets_provider`
            secrets_provider=None,
            project_settings=self.config.project_settings(),
            stack_settings=self.config.stack_settings(),
        )
        # initial state
        self._pulumi_program_cache = {}

    async def preview(self, *, processor: Processor) -> WrappedPreviewResult:
        logging.debug(
            f"Pulumi Preview: {self.config.project_name}:{self.config.stack_name}"
        )
        stack = self._create_or_select_stack(processor)
        return WrappedPreviewResult(preview_result=stack.preview())

    async def up(self, *, processor: Processor) -> WrappedUpResult:
        logging.debug(f"Pulumi Up: {self.config.project_name}:{self.config.stack_name}")
        stack = self._create_or_select_stack(processor)
        return WrappedUpResult(up_result=stack.up())

    async def outputs(self, *, processor: Processor) -> WrappedOutputMap:
        logging.debug(
            f"Pulumi Outputs: {self.config.project_name}:{self.config.stack_name}"
        )
        stack = self._create_or_select_stack(processor)
        return WrappedOutputMap(output_map=stack.outputs())

    async def destroy(self, *, processor: Processor) -> WrappedDestroyResult:
        logging.debug(
            f"Pulumi Destroy: {self.config.project_name}:{self.config.stack_name}"
        )  # noqa: E501
        stack = self._create_or_select_stack(processor)
        return WrappedDestroyResult(destroy_result=stack.destroy())

    def _create_pulumi_program(self, processor: Processor):
        def pulumi_program():
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
        if self.config.stack_name not in self._pulumi_program_cache:
            pulumi_program = self._create_pulumi_program(processors)
            self._pulumi_program_cache[self.config.stack_name] = pulumi_program
        else:
            pulumi_program = self._pulumi_program_cache[self.config.stack_name]

        return auto.create_or_select_stack(
            stack_name=self.config.stack_name,
            project_name=self.config.project_name,
            program=pulumi_program,
            opts=self.workspace_options,
        )
