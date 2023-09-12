import logging
from typing import Callable

from buildflow.config.pulumi_config import PulumiConfig
from buildflow.core.app.infra._infra import Infra, InfraStatus
from buildflow.core.app.infra.pulumi_workspace import (
    PulumiWorkspace,
    WrappedDestroyResult,
    WrappedOutputMap,
    WrappedPreviewResult,
    WrappedRefreshResult,
    WrappedUpResult,
)
from buildflow.core.options.infra_options import InfraOptions


class InfraActor(Infra):
    def __init__(
        self,
        infra_options: InfraOptions,
        pulumi_config: PulumiConfig,
    ) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(infra_options.log_level)

        # configuration
        self.options = infra_options
        # initial infra state
        self._status = InfraStatus.IDLE
        self._pulumi_workspace = PulumiWorkspace(
            infra_options.pulumi_options,
            pulumi_config,
        )

    def _set_status(self, status: InfraStatus):
        self._status = status

    async def refresh(self, *, pulumi_program: Callable):
        logging.debug("Refreshing Infra...")
        if self._status != InfraStatus.IDLE:
            raise RuntimeError("Can only refresh Infra while Idle.")
        self._set_status(InfraStatus.REFRESHING)

        refresh_result: WrappedRefreshResult = await self._pulumi_workspace.refresh(
            pulumi_program=pulumi_program,
        )
        refresh_result.log_summary()

        self._set_status(InfraStatus.IDLE)

    async def plan(self, *, pulumi_program: Callable):
        logging.debug("Planning Infra...")
        if self._status != InfraStatus.IDLE:
            raise RuntimeError("Can only plan Infra while Idle.")
        self._set_status(InfraStatus.PLANNING)

        preview_result: WrappedPreviewResult = await self._pulumi_workspace.preview(
            pulumi_program=pulumi_program,
        )
        preview_result.log_summary()
        preview_result.print_change_summary()

        self._set_status(InfraStatus.IDLE)

    async def apply(self, *, pulumi_program: Callable):
        logging.info("Applying Infra...")
        if self._status != InfraStatus.IDLE:
            raise RuntimeError("Can only apply Infra while Idle.")
        self._set_status(InfraStatus.APPLYING)

        # Execution phase (potentially remote state changes)
        up_result: WrappedUpResult = await self._pulumi_workspace.up(
            pulumi_program=pulumi_program,
        )
        up_result.log_summary()

        self._set_status(InfraStatus.IDLE)

    async def destroy(self, *, pulumi_program: Callable):
        logging.info("Destroying Infra...")
        if self._status != InfraStatus.IDLE:
            raise RuntimeError("Can only destroy Infra while Idle.")
        self._set_status(InfraStatus.DESTROYING)

        # Planning phase (no remote state changes)
        output_map: WrappedOutputMap = await self._pulumi_workspace.outputs(
            pulumi_program=pulumi_program,
        )
        if self.options.require_confirmation:
            print("Would you like to delete this infra?")
            output_map.print_summary()
            response = input('Enter "y (yes)" to confirm, "n (no) to reject": ')
            while True:
                if response.lower() in ["n", "no"]:
                    print("User rejected Infra changes. Aborting.")
                    return
                elif response.lower() in ["y", "yes"]:
                    print("User confirmed Infra changes. Destroying.")
                    break
                else:
                    response = input(
                        'Invalid response. Enter "y (yes)" to '
                        'confirm, "n (no) to reject": '
                    )

        # Execution phase (potentially remote state changes)
        destroy_result: WrappedDestroyResult = await self._pulumi_workspace.destroy(
            pulumi_program=pulumi_program,
        )
        destroy_result.log_summary()

        self._set_status(InfraStatus.IDLE)

    def is_active(self):
        return self._status != InfraStatus.IDLE
