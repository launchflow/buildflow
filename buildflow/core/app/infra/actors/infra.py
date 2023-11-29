import logging
from collections import OrderedDict
from typing import Any, Callable, Dict

from buildflow.config.pulumi_config import PulumiConfig
from buildflow.core.app.infra._infra import Infra
from buildflow.core.app.infra.pulumi_workspace import (
    PulumiWorkspace,
    WrappedPreviewResult,
    WrappedRefreshResult,
)
from buildflow.core.options.infra_options import InfraOptions
from buildflow.io.primitive import Primitive


def _print_hierarchy(data: Dict[str, Any], prefix: str = ""):
    if isinstance(data, dict):
        for idx, (key, value) in enumerate(data.items()):
            is_last = idx == len(data) - 1
            joint = "└── " if is_last else "├── "
            new_prefix = prefix + ("    " if is_last else "│   ")
            print(f"{prefix}{joint}{key}")
            _print_hierarchy(value, new_prefix)
    elif isinstance(data, list):
        for idx, value in enumerate(data):
            is_last = idx == len(data) - 1
            _print_hierarchy(value, prefix)
    else:
        print(f"{prefix}└── {data}")


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
        self._pulumi_workspace = PulumiWorkspace(
            infra_options.pulumi_options,
            pulumi_config,
        )

    async def stack_state(self):
        return self._pulumi_workspace.get_stack_state()

    async def refresh(self, *, pulumi_program: Callable):
        logging.debug("Refreshing Infra...")
        refresh_result: WrappedRefreshResult = await self._pulumi_workspace.refresh(
            pulumi_program=pulumi_program,
        )
        refresh_result.log_summary()

    async def preview(
        self, *, pulumi_program: Callable, managed_primitives: Dict[str, Primitive]
    ):
        logging.debug("Planning Infra...")

        preview_result: WrappedPreviewResult = await self._pulumi_workspace.preview(
            pulumi_program=pulumi_program,
        )
        stack_state = await self.stack_state()
        stack_resources = stack_state.resources()
        plan_result = preview_result.plan_result
        update_plans = {}
        for urn, resource_plan in plan_result["resourcePlans"].items():
            if "update" in resource_plan["steps"]:
                update_plans[urn] = resource_plan
            # TODO: should we handle other things? guessing replace
            # https://github.com/pulumi/pulumi/blob/master/sdk/go/common/apitype/history.go#L63

        destroyed_primitives = OrderedDict()
        existing_primitives = OrderedDict()
        for resource in stack_resources.values():
            if "primitive_id" in resource.resource_outputs:
                if resource.resource_outputs["primitive_id"] not in managed_primitives:
                    prim_type = resource.resource_outputs["primitive_type"]
                    if prim_type in destroyed_primitives:
                        destroyed_primitives[prim_type].append(
                            resource.resource_outputs["primitive_id"]
                        )
                    else:
                        destroyed_primitives[
                            resource.resource_outputs["primitive_type"]
                        ] = [resource.resource_outputs["primitive_id"]]
                existing_primitives[resource.resource_outputs["primitive_id"]] = True

        primitives_to_create = OrderedDict()
        for primitive_id, prim in managed_primitives.items():
            if primitive_id not in existing_primitives:
                prim_type = type(prim).__name__
                if prim_type in primitives_to_create:
                    primitives_to_create[prim_type].append(prim.primitive_id())
                else:
                    primitives_to_create[type(prim).__name__] = [prim.primitive_id()]

        update_primitives = OrderedDict()
        for update_urn in update_plans.keys():
            if update_urn not in stack_resources:
                raise ValueError(
                    "Could not find an update in the stack state."
                    " This is an invalid state."
                )
            resource = stack_resources[update_urn]
            while True:
                if "primitive_id" in resource.resource_outputs:
                    diff = list(
                        update_plans[update_urn]["goal"]["inputDiff"]
                        .get("updates", {})
                        .keys()
                    )
                    diff.extend(
                        update_plans[update_urn]["goal"]["inputDiff"]
                        .get("adds", {})
                        .keys()
                    )
                    if (
                        resource.resource_outputs["primitive_id"]
                        not in managed_primitives
                    ):
                        # This can happen if there is a pending update to a resource
                        # that is being deleted.
                        break
                    prim = managed_primitives[resource.resource_outputs["primitive_id"]]
                    prim_type = type(prim).__name__
                    if prim_type in update_primitives:
                        update_primitives[prim_type].append({prim.primitive_id(): diff})
                    else:
                        update_primitives[prim_type] = [{prim.primitive_id(): diff}]
                    break
                elif resource.parent:
                    resource = stack_resources[resource.parent]
                else:
                    logging.info(
                        "Failed to find primitive ID for: %s of type: %s. This maybe "
                        "expected if the resource is not managed by Buildflow.",
                        resource.resource_id,
                        resource.resource_type,
                    )
                    break
        if primitives_to_create:
            print("Primitives to create:")
            print("---------------------")
            _print_hierarchy(primitives_to_create)
            print("\n")
        if destroyed_primitives:
            print("Primitives to destroy:")
            print("----------------------")
            _print_hierarchy(destroyed_primitives)
            print("\n")
        if update_primitives:
            print("Primitives to update:")
            print("---------------------")
            _print_hierarchy(update_primitives)
            print("\n")

        return preview_result

    async def apply(self, *, pulumi_program: Callable):
        # Execution phase (potentially remote state changes)
        await self._pulumi_workspace.up(
            pulumi_program=pulumi_program,
        )

    async def destroy(self, *, pulumi_program: Callable):
        # Execution phase (potentially remote state changes)
        await self._pulumi_workspace.destroy(
            pulumi_program=pulumi_program,
        )
