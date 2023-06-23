import asyncio
import dataclasses
import logging
from typing import Dict, Iterable, List

import ray

from buildflow import utils
from buildflow.api import InfraAPI, StateAPI, InfraStatus, InfraTag
from buildflow.api.processor import ProcessorID
from buildflow.core.processor.base import Processor
from buildflow.core.infra.actors.pulumi_workspace import (
    PulumiWorkspaceActor,
    WrappedDestroyResult,
    WrappedOutputMap,
    WrappedPreviewResult,
    WrappedUpResult,
)
from buildflow.core.infra.config import InfraConfig


@dataclasses.dataclass
class ResourceState(StateAPI):
    pass

    def as_dict(self):
        return {}


@dataclasses.dataclass
class InfraState(StateAPI):
    status: InfraStatus = InfraStatus.IDLE
    resources: List[ResourceState] = dataclasses.field(default_factory=list)

    _timestamp_millis: int = dataclasses.field(default_factory=utils.timestamp_millis)

    def get_timestamp_millis(self) -> int:
        return self._timestamp_millis

    def as_dict(self):
        return {
            "status": self.status.name,
            "timestamp_millis": self.get_timestamp_millis(),
            "resources": [r.as_dict() for r in self.resources],
        }


@ray.remote
class InfraActor(InfraAPI):
    def __init__(self, config: InfraConfig, tag: InfraTag) -> None:
        # NOTE: Ray actors run in their own process, so we need to configure
        # logging per actor / remote task.
        logging.getLogger().setLevel(config.log_level)

        # set configuration
        self.config = config
        self.tag = tag
        # initial infra state
        self._status = InfraStatus.IDLE
        self._pulumi_workspace_actors: Dict[ProcessorID, PulumiWorkspaceActor] = {}

    def _get_workspace_actor_for_processor(
        self, processor_id: ProcessorID
    ) -> PulumiWorkspaceActor:
        if processor_id not in self._pulumi_workspace_actors:
            if processor_id not in self.config.pulumi_workspace_configs:
                raise RuntimeError(
                    f"Processor {processor_id} is not configured in Infra config."
                )
            workspace_config = self.config.pulumi_workspace_configs[processor_id]
            self._pulumi_workspace_actors[processor_id] = PulumiWorkspaceActor.remote(
                config=workspace_config
            )
        return self._pulumi_workspace_actors[processor_id]

    async def plan(self, *, processors: Iterable[Processor]):
        logging.debug("Planning Infra...")
        if self._status != InfraStatus.IDLE:
            raise RuntimeError("Can only plan Infra while Idle.")
        self._status = InfraStatus.PLANNING

        preview_result_tasks = []
        for processor in processors:
            pulumi_workspace_actor = self._get_workspace_actor_for_processor(
                processor_id=processor.processor_id
            )
            preview_result_tasks.append(
                pulumi_workspace_actor.plan.remote(processor=processor)
            )

        preview_results: List[WrappedPreviewResult] = asyncio.gather(
            *preview_result_tasks
        )

        for preview_result in preview_results:
            preview_result.log_summary()

        self._status = InfraStatus.IDLE

    async def apply(self, *, processors: Iterable[Processor]):
        logging.info("Applying Infra...")
        if self._status != InfraStatus.IDLE:
            raise RuntimeError("Can only apply Infra while Idle.")
        self._status = InfraStatus.APPLYING

        # Planning phase (no remote state changes)
        preview_result: WrappedPreviewResult = (
            await self._pulumi_workspace_actor.preview.remote(processors=processors)
        )
        if self.config.require_confirmation:
            print("Would you like to apply these changes?")
            print(preview_result.change_summary)
            response = input('Enter "yes" to confirm: ')
            if response != "yes":
                print("User did not confirm Infra changes. Aborting.")
                return
            print("User confirmed Infra changes. Applying.")

        logging.warning(f"apply: Applying: {preview_result.change_summary}")

        # Execution phase (potentially remote state changes)
        up_result: WrappedUpResult = await self._pulumi_workspace_actor.up.remote(
            processors=processors
        )
        logging.info(f"apply: {up_result.stdout}")
        if up_result.stderr:
            logging.error(f"apply: {up_result.stderr}")
        logging.info(f"apply: {up_result.summary}")
        logging.info(f"apply: {up_result.outputs}")

        self._status = InfraStatus.IDLE

    async def destroy(self, *, processors: Iterable[Processor]):
        logging.info("Destroying Infra...")
        if self._status != InfraStatus.IDLE:
            raise RuntimeError("Can only destroy Infra while Idle.")
        self._status = InfraStatus.DESTROYING
        self._update_workspace_actors(processors)

        # Planning phase (no remote state changes)
        outputs_map: WrappedOutputMap = (
            await self._pulumi_workspace_actor.outputs.remote(processors=processors)
        )
        if self.config.require_confirmation:
            print("Would you like to delete this infra?")
            print(outputs_map)
            response = input('Enter "yes" to confirm: ')
            if response != "yes":
                print("User did not confirm Infra changes. Aborting.")
                return
            print("User confirmed Infra changes. Destroying.")

        logging.warning(f"destroy: Removing: {outputs_map}")

        # Execution phase (potentially remote state changes)
        destroy_result: WrappedDestroyResult = (
            await self._pulumi_workspace_actor.destroy.remote(
                processors=processors, outputs_map=outputs_map
            )
        )
        logging.info(f"destroy: {destroy_result.stdout}")
        if destroy_result.stderr:
            logging.error(f"destroy: {destroy_result.stderr}")
        logging.info(f"destroy: {destroy_result.summary}")

        self._status = InfraStatus.IDLE

    def is_active(self):
        return self._status != InfraStatus.IDLE
