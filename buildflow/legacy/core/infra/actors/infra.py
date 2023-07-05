import logging
from typing import Iterable, Optional

import ray

from buildflow.api import InfraAPI, InfraStatus
from buildflow.core.infra.actors.pulumi_workspace import (
    PulumiWorkspaceActor,
    WrappedDestroyResult,
    WrappedOutputMap,
    WrappedPreviewResult,
    WrappedUpResult,
)
from buildflow.core.infra.options import InfraOptions
from buildflow.core.processor.base import Processor


@ray.remote
class InfraActor(InfraAPI):
    def __init__(
        self,
        infra_options: InfraOptions,
        on_status_change: Optional[callable] = None,
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(infra_options.log_level)

        # configuration
        self.options = infra_options
        self.on_status_change = on_status_change
        # initial infra state
        self._status = InfraStatus.IDLE
        self._pulumi_workspace_actor = PulumiWorkspaceActor.remote(
            infra_options.pulumi_options
        )

    def _set_status(self, status: InfraStatus):
        if self.on_status_change is not None:
            self.on_status_change(status)
        self._status = status

    async def plan(self, *, processors: Iterable[Processor]):
        logging.debug("Planning Infra...")
        if self._status != InfraStatus.IDLE:
            raise RuntimeError("Can only plan Infra while Idle.")
        self._set_status(InfraStatus.PLANNING)

        preview_result: WrappedPreviewResult = (
            await self._pulumi_workspace_actor.preview.remote(processors=processors)
        )
        preview_result.log_summary()
        preview_result.print_change_summary()

        self._set_status(InfraStatus.IDLE)

    async def apply(self, *, processors: Iterable[Processor]):
        logging.info("Applying Infra...")
        if self._status != InfraStatus.IDLE:
            raise RuntimeError("Can only apply Infra while Idle.")
        self._set_status(InfraStatus.APPLYING)

        # Planning phase (no remote state changes)
        preview_result: WrappedPreviewResult = (
            await self._pulumi_workspace_actor.preview.remote(processors=processors)
        )
        if self.options.require_confirmation:
            print("Would you like to apply these changes?")
            preview_result.print_change_summary()
            response = input('Enter "yes" to confirm: ')
            if response != "yes":
                print("User did not confirm Infra changes. Aborting.")
                return
            print("User confirmed Infra changes. Applying.")

        # TODO: Aggregate all change summaries into a single summary and log it.
        # logging.warning(f"apply: Applying: {preview_result.change_summary}")

        # Execution phase (potentially remote state changes)
        up_result: WrappedUpResult = await self._pulumi_workspace_actor.up.remote(
            processors=processors
        )
        up_result.log_summary()

        self._set_status(InfraStatus.IDLE)

    async def destroy(self, *, processors: Iterable[Processor]):
        logging.info("Destroying Infra...")
        if self._status != InfraStatus.IDLE:
            raise RuntimeError("Can only destroy Infra while Idle.")
        self._set_status(InfraStatus.DESTROYING)

        # Planning phase (no remote state changes)
        output_map: WrappedOutputMap = (
            await self._pulumi_workspace_actor.outputs.remote(processors=processors)
        )
        if self.options.require_confirmation:
            print("Would you like to delete this infra?")
            output_map.print_summary()
            response = input('Enter "yes" to confirm: ')
            if response != "yes":
                print("User did not confirm Infra changes. Aborting.")
                return
            print("User confirmed Infra changes. Destroying.")

        # TODO: Aggregate all outputs_maps into a single summary and log it.
        # logging.warning(f"destroy: Removing: {outputs_map}")

        # Execution phase (potentially remote state changes)
        destroy_result: WrappedDestroyResult = (
            await self._pulumi_workspace_actor.destroy.remote(processors=processors)
        )
        destroy_result.log_summary()

        self._set_status(InfraStatus.IDLE)

    def is_active(self):
        return self._status != InfraStatus.IDLE
